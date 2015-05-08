/*
    Copyright 2014 - Justin Weiler (justin@justinweiler.com)

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;

namespace Phrenologix
{
    public class Tests
    {
        // Basic multi threaded access tests for data consistency and save and load
        public static void BloomFileSmokeTest1(int iteration)
        {
            int numThreads = 4; // make an even number
            int million = 1000000;
            int pktSz = 180;
            int keysToCreateCount = million * numThreads;
            int keysToSaveCount = million;
            DateTime timeStart = DateTime.Now;
            var savedKeysList = new List<_keyTracker>(million);
            int modSave = keysToCreateCount / million;
            float pktSzSlack = 1.0f;
            bool doComputeChecksum = false;
            string filePath = @"c:\data\test";

            // clean data
            _cleanData(Path.GetDirectoryName(filePath));

            // create bf
            var bf = _createBloomFile(filePath, iteration, pktSz, pktSzSlack, doComputeChecksum);

            // mixed MT operations (write, read, update)
            _bulkMixedMT(bf, iteration, numThreads, pktSz, keysToCreateCount, modSave, keysToSaveCount, savedKeysList, 30, 50, 19, 1, 5);
            bf.Dispose();

            // sort keys for randomization sake
            savedKeysList.Sort();

            // load whole bf
            bf = _loadBloomFile(filePath, iteration);

            // random read keys - multithreaded
            _bulkReadKeysMTFromSavedKeysList(bf, iteration, numThreads, savedKeysList.Count, savedKeysList);
            bf.Dispose();
        }

        // Basic single threaded access tests for data consistency and save and load
        public static void BloomFileSmokeTest2(int iteration)
        {
            int million = 1000000;
            int pktSz = 100;
            DateTime timeStart = DateTime.Now;
            var savedKeysList = new List<HashKey>(million);
            int keysToCreateOrUpdateCount = million * 20;
            int keysToSaveCount = million;
            int modSave = (keysToCreateOrUpdateCount / million) - 1;
            float pktSzSlack = 1.0f;
            bool doComputeChecksum = false;
            string filePath = @"c:\data\test";
            string keyFilePath = @"c:\data\keys.dat";

            // clean data
            _cleanData(filePath);

            // create key file
            _createKeyFile(keyFilePath, iteration, keysToCreateOrUpdateCount, false);

            // create bf
            var bf = _createBloomFile(filePath, iteration, pktSz, pktSzSlack, doComputeChecksum);

            // bulk create from key file
            _bulkCreateFromKeyFile(keyFilePath, bf, iteration, pktSz, 0, keysToCreateOrUpdateCount, modSave, savedKeysList);
            bf.Dispose();

            // load half bf
            bf = _loadBloomFile(filePath, iteration);

            // bulk update from key file (doubles size of bf)
            _bulkUpdateFromKeyFile(keyFilePath, bf, iteration, pktSz, 0, keysToCreateOrUpdateCount);
            bf.Dispose();

            // sort keys for randomization sake
            savedKeysList.Sort();

            // load whole bf
            bf = _loadBloomFile(filePath, iteration);

            // read saved keys
            _bulkReadKeysFromSavedKeysList(bf, iteration, savedKeysList);
            bf.Dispose();
        }

        // Basic capacity test
        public static void HashKeyCapacityTest()
        {
            // set filter properties
            int million = 1000000;
            int count = 0;
            int getCnt = 0;
            int capacity = 954;
            float factor = 1.3F; // 1.0 is perfectly sparse, less is overpacked
            float errorRate = 0.001F;
            Queue<HashKey> savedKeys = new Queue<HashKey>(million);
            Random rnd = new Random();
            long mem = GC.GetTotalMemory(true);
            var bloomFileBranchList = new List<bloomFileBranch>();
            DateTime timeStart = DateTime.Now;

            Console.WriteLine("Start: " + timeStart);

            bloomFileBranch.branchFactors = new byte[] { 0, 4, 8, 16, 32 };

            for (int n = 0; n < 64; n++)
            {
                bloomFileBranch L5 = new bloomFileBranch(0, 5, capacity * (int)((float)32 * (float)16 * (float)8 * (float)4 * factor), errorRate);
                bloomFileBranchList.Add(L5);

                for (int m = 0; m < 32; m++)
                {
                    Console.WriteLine("Loop: {0}.{1}", n, m);

                    bloomFileBranch L4 = new bloomFileBranch(0, 4, capacity * (int)((float)16 * (float)8 * (float)4 * factor), errorRate);
                    L5.addChildBranch(L4);

                    for (int l = 0; l < 16; l++)
                    {
                        bloomFileBranch L3 = new bloomFileBranch(0, 3, capacity * (int)((float)8 * (float)4 * factor), errorRate);
                        L4.addChildBranch(L3);

                        for (int k = 0; k < 8; k++)
                        {
                            bloomFileBranch L2 = new bloomFileBranch(0, 2, capacity * (int)((float)4 * factor), errorRate);
                            L3.addChildBranch(L2);

                            for (int j = 0; j < 4; j++)
                            {
                                //bloomFileBlossom L1 = new bloomFileBlossom(null, null, 0, branch, (int)((float)capacity * finalfactor * (float)bonsai), errorRate, false);
                                bloomFileBranch L1 = new bloomFileBranch(0, 1, (int)((float)capacity * factor), errorRate);

                                L2.addChildBranch(L1);

                                for (int i = 0; i < capacity; i++)
                                {
                                    var key = new HashKey();
                                    key.GenerateRandomKey();

                                    L1.superAddKey(key);

                                    var rndItem = rnd.Next();

                                    // save key for later lookup
                                    if (rndItem % 50 == 0 && savedKeys.Count < million)
                                    {
                                        savedKeys.Enqueue(key);
                                    }

                                    // lookup key
                                    if (rndItem % 99 == 0 && savedKeys.Count > 0)
                                    {
                                        var getKey = savedKeys.Dequeue();

                                        Status foundBranch = Status.KeyNotFound;

                                        foreach (var bloomFileFilter in bloomFileBranchList)
                                        {
                                            foundBranch = bloomFileFilter.superContainsKey(getKey, null, null);

                                            if (foundBranch == Status.Successful)
                                            {
                                                break;
                                            }
                                        }

                                        if (foundBranch == Status.KeyNotFound)
                                        {
                                            throw new Exception("Did not find valid key");
                                        }

                                        getCnt++;
                                    }

                                    count++;

                                    if (count % million == 0)
                                    {
#if TRACKFP
                                        _countFalsePositives(bloomFileBranchList, getCnt);
#endif

                                        Console.WriteLine(string.Format("{0} added, {1} biased read, time delta: {2}", count, getCnt, DateTime.Now - timeStart));
                                    }
                                }
                            }
                        }
                    }
                }
            }

            Console.WriteLine("memory delta: {0}", GC.GetTotalMemory(true) - mem);
            Console.WriteLine(string.Format("{0} added, time delta: {1}", count, DateTime.Now - timeStart));

            var sw = new Stopwatch();

            sw.Start();

#if TRACKFP
            _clearFalsePositives(bloomFileBranchList);
#endif

            // check for a bunch of non-existent keys
            var loop = million;

            int falsePositives = 0;

            while (loop-- > 0)
            {
                var key = new HashKey();
                key.GenerateRandomKey();

                foreach (var bloomFileFilter in bloomFileBranchList)
                {
                    if (bloomFileFilter.superContainsKey(key, null, null) != Status.KeyNotFound)
                    {
                        Console.WriteLine(string.Format("Found {0} keys that don't exist!", ++falsePositives));
                    }
                }
            }

            sw.Stop();

#if TRACKFP
            _countFalsePositives(bloomFileBranchList, million);
#endif

            Console.WriteLine(string.Format("End: {0} - {1} (ms) capacity positive/negative", DateTime.Now, sw.ElapsedMilliseconds));

            Console.ReadLine();
        }

        #region Private Implementation

        private static bool[] _doStats;

        private static int _itemsCreated;
        private static int _itemsRead;
        private static int _itemsUpdated;
        private static int _itemsDeleted;

        private static long _createLatency;
        private static long _readLatency;
        private static long _updateLatency;
        private static long _deleteLatency;

        private static int _totalOps;

        private static Stopwatch _stw = new Stopwatch();

        private class _keyTracker : IComparable
        {
            internal HashKey key;
            internal int version;
            internal bool deleted;

            public int CompareTo(object obj)
            {
                return key.CompareTo((obj as _keyTracker).key);
            }
        }

        private class _threadMixedVars
        {
            internal int pktSz;
            internal List<_keyTracker> savedKeysList = new List<_keyTracker>();
            internal BloomFile bf;
            internal int iteration;
            internal int keysToCreateCount;
            internal int modSave;
            internal int keysToSaveCount;
            internal Barrier barrier;
            internal int[] pctTable;
            internal int biasPct;
            internal int tID;
        }

        private class _threadReadVars
        {
            internal List<_keyTracker> savedKeysList;
            internal BloomFile bf;
            internal int iteration;
            internal Barrier barrier;
            internal int tID;
        }

#if TRACKFP
        private static void _clearFalsePositives(List<bloomFileBranch> bloomFileBranchList)
        {
            foreach (var L5 in bloomFileBranchList)
            {
                L5.clearFalsePositives();
            }
        }

        private static void _countFalsePositives(List<bloomFileBranch> bloomFileBranchList, int totalAttempts)
        {
            int falsePositives = 0;

            foreach (var L5 in bloomFileBranchList)
            {
                falsePositives += L5.countFalsePositives(5);
            }

            Console.WriteLine(string.Format("L5: {0} of {1}", falsePositives, totalAttempts));

            falsePositives = 0;

            foreach (var L5 in bloomFileBranchList)
            {
                falsePositives += L5.countFalsePositives(4);
            }

            Console.WriteLine(string.Format("L4: {0} of {1}", falsePositives, totalAttempts));

            falsePositives = 0;

            foreach (var L5 in bloomFileBranchList)
            {
                falsePositives += L5.countFalsePositives(3);
            }

            Console.WriteLine(string.Format("L3: {0} of {1}", falsePositives, totalAttempts));

            falsePositives = 0;

            foreach (var L5 in bloomFileBranchList)
            {
                falsePositives += L5.countFalsePositives(2);
            }

            Console.WriteLine(string.Format("L2: {0} of {1}", falsePositives, totalAttempts));

            falsePositives = 0;

            foreach (var L5 in bloomFileBranchList)
            {
                falsePositives += L5.countFalsePositives(1);
            }

            Console.WriteLine(string.Format("L1: {0} of {1}", falsePositives, totalAttempts));
        }
#endif

        private static void _startFlushing(object sender, EventArgs e)
        {
            //Console.WriteLine("Start flushing all trees: {0}", DateTime.Now);
        }

        private static void _stopFlushing(object sender, EventArgs e)
        {
            //Console.WriteLine("Stop flushing all trees: {0}", DateTime.Now);
        }

        private static List<HashKey> _createKeyFile(string keyFilePath, int iteration, int keysToAddCount, bool makeFullKeyList)
        {
            List<HashKey> keyList = null;

            var timeStart = DateTime.Now;

            Console.WriteLine(string.Format("Iteration: {0} Key Generation Start: {1}", iteration, timeStart));

            if (File.Exists(keyFilePath) == true)
            {
                File.Delete(keyFilePath);
            }

            if (makeFullKeyList == true)
            {
                keyList = new List<HashKey>();
            }

            var fs = File.Create(keyFilePath);

            using (var bw = new BinaryWriter(fs))
            {
                for (int i = 0; i < keysToAddCount; i++)
                {
                    var key = new HashKey();
                    key.GenerateRandomKey();
                    bw.Write(key.uint1);
                    bw.Write(key.uint2);
                    bw.Write(key.uint3);
                    bw.Write(key.uint4);
                    bw.Write(key.uint5);

                    if (keyList != null)
                    {
                        keyList.Add(key);
                    }
                }

                bw.Flush();
            }

            fs = null;

            Console.WriteLine(string.Format("Iteration: {0} Key Generation Complete: {1} Keys Created. Time Delta: {2}", iteration, keysToAddCount, DateTime.Now - timeStart));

            return keyList;
        }

        private static void _cleanData(string directoryName)
        {
            if (Directory.Exists(directoryName) == true)
            {
                Directory.Delete(directoryName, true);
            }

            Directory.CreateDirectory(directoryName);
        }

        private static void _startCounting()
        {
            _stw.Restart();

            _itemsCreated = 0;
            _itemsRead = 0;
            _itemsUpdated = 0;
            _itemsDeleted = 0;

            _createLatency = 0;
            _readLatency = 0;
            _updateLatency = 0;
            _deleteLatency = 0;

            _totalOps = 0;
        }

        private static BloomFile _createBloomFile(string filePath, int iteration, int pktSz, float pktSzSlack, bool doComputeChecksum)
        {
            long mem = GC.GetTotalMemory(true);
            var timeStart = DateTime.Now;

            Console.WriteLine(string.Format("Iteration: {0} Create BloomFile Start: {1}", iteration, timeStart));

            var bf = BloomFile.CreateBloomFile(filePath, pktSz, pktSzSlack, doComputeChecksum);

            bf.OnFlushingStart += _startFlushing;
            bf.OnFlushingStop += _stopFlushing;

            Console.WriteLine(string.Format("Iteration: {0} Create BloomFile Complete: {1} Memory Delta: {2}", iteration, DateTime.Now, GC.GetTotalMemory(true) - mem));

            return bf;
        }

        private static void _bulkCreateFromKeyFile(string keyFilePath, BloomFile bf, int iteration, int pktSz, int keyStartNumber, int keysToCreate, int modSave, List<HashKey> savedKeysList)
        {
            int million = 1000000;
            long mem = GC.GetTotalMemory(true);
            var timeStart = DateTime.Now;
            var bufferBytes = new byte[pktSz];
            var key = new HashKey();

            Console.WriteLine(string.Format("Iteration: {0} Bulk Creates Start: {1}", iteration, timeStart));

            unsafe
            {
                fixed (byte* bufferBytesPtr = bufferBytes)
                {
                    var fs = File.OpenRead(keyFilePath);

                    fs.Position = keyStartNumber * 20;

                    using (var br = new BinaryReader(fs))
                    {
                        for (int i = 1; i <= keysToCreate; i++)
                        {
                            key.uint1 = br.ReadUInt32();
                            key.uint2 = br.ReadUInt32();
                            key.uint3 = br.ReadUInt32();
                            key.uint4 = br.ReadUInt32();
                            key.uint5 = br.ReadUInt32();

                            *((uint*)(bufferBytesPtr + 0)) = key.uint1;
                            *((uint*)(bufferBytesPtr + 4)) = key.uint2;
                            *((uint*)(bufferBytesPtr + 8)) = key.uint3;
                            *((uint*)(bufferBytesPtr + 12)) = key.uint4;
                            *((uint*)(bufferBytesPtr + 16)) = key.uint5;

                            if (modSave > 0 && i % modSave == 0 && savedKeysList.Count < million)
                            {
                                savedKeysList.Add(key);
                            }

                            var result = bf.Create(key, bufferBytes, 42, DateTime.MaxValue, 69, false);

                            if (result != Status.Successful)
                            {
                                throw new ApplicationException(string.Format("Unable to create key: {0} [{1}]", key, result));
                            }

                            if (i % million == 0)
                            {
                                Console.WriteLine(string.Format("Iteration: {0} Created: {1} Time Delta: {2}", iteration, i + keyStartNumber, DateTime.Now - timeStart));
                            }
                        }
                    }

                    fs = null;
                }
            }

            Console.WriteLine(string.Format("Iteration: {0} Bulk Creates Complete: {1} Memory Delta: {2}", iteration, DateTime.Now, GC.GetTotalMemory(true) - mem));
        }

        private static void _bulkUpdateFromKeyFile(string keyFilePath, BloomFile bf, int iteration, int pktSz, int keyStartNumber, int keysToUpdate)
        {
            int million = 1000000;
            long mem = GC.GetTotalMemory(true);
            var timeStart = DateTime.Now;
            var bufferBytes = new byte[pktSz];
            var key = new HashKey();

            Console.WriteLine(string.Format("Iteration: {0} Bulk Updates Start: {1}", iteration, timeStart));

            unsafe
            {
                fixed (byte* bufferBytesPtr = bufferBytes)
                {
                    var fs = File.OpenRead(keyFilePath);

                    fs.Position = keyStartNumber * 20;

                    using (var br = new BinaryReader(fs))
                    {
                        for (int i = 1; i <= keysToUpdate; i++)
                        {
                            key.uint1 = br.ReadUInt32();
                            key.uint2 = br.ReadUInt32();
                            key.uint3 = br.ReadUInt32();
                            key.uint4 = br.ReadUInt32();
                            key.uint5 = br.ReadUInt32();

                            *((uint*)(bufferBytesPtr + 0)) = key.uint1;
                            *((uint*)(bufferBytesPtr + 4)) = key.uint2;
                            *((uint*)(bufferBytesPtr + 8)) = key.uint3;
                            *((uint*)(bufferBytesPtr + 12)) = key.uint4;
                            *((uint*)(bufferBytesPtr + 16)) = key.uint5;

                            DateTime timestamp = DateTime.MaxValue;
                            int versionNo = 0;
                            var result = bf.Update(key, bufferBytes, 42, ref timestamp, ref versionNo, true);

                            if (result != Status.Successful)
                            {
                                throw new ApplicationException(string.Format("Unable to update key: {0} [{1}]", key, result));
                            }

                            if (i % million == 0)
                            {
                                Console.WriteLine(string.Format("Iteration: {0} Updated: {1} Time Delta: {2}", iteration, i + keyStartNumber, DateTime.Now - timeStart));
                            }
                        }
                    }

                    fs = null;
                }
            }

            Console.WriteLine(string.Format("Iteration: {0} Bulk Updates Complete: {1} Memory Delta: {2}", iteration, DateTime.Now, GC.GetTotalMemory(true) - mem));
        }

        private static void _bulkMixedMT(BloomFile bf, int iteration, int numThreads, int pktSz, int keysToCreateCount, int modSave, int keysToSaveCount,
            List<_keyTracker> savedKeysList, int createPct, int readPct, int updatePct, int deletePct, int biasPct)
        {
            long mem = GC.GetTotalMemory(true);
            var timeStart = DateTime.Now;

            Console.WriteLine(string.Format("Iteration: {0} Bulk Mixed Start: {1}", iteration, timeStart));

            var threads = new Thread[numThreads];
            var threadMixedVarsArray = new _threadMixedVars[numThreads];
            var barrier = new Barrier(numThreads);

            // generate pct table
            List<char> tempList = new List<char>();
            int[] pctTable = new int[100];
            int pos = 0;

            for (int i = 0; i < createPct; i++)
            {
                tempList.Add('C');
            }

            for (int i = 0; i < readPct; i++)
            {
                tempList.Add('R');
            }

            for (int i = 0; i < updatePct; i++)
            {
                tempList.Add('U');
            }

            for (int i = 0; i < deletePct; i++)
            {
                tempList.Add('D');
            }

            var rnd = new Random();

            for (int i = tempList.Count; i > 0; i--)
            {
                var idx = rnd.Next(i);
                pctTable[pos++] = tempList[idx];
                tempList.RemoveAt(idx);
            }

            _doStats = new bool[numThreads];

            for (int i = 0; i < numThreads; i++)
            {
                var threadMixedVars = new _threadMixedVars
                {
                    pktSz = pktSz,
                    bf = bf,
                    iteration = iteration,
                    keysToSaveCount = keysToSaveCount / numThreads,
                    keysToCreateCount = keysToCreateCount / numThreads,
                    modSave = modSave,
                    barrier = barrier,
                    pctTable = pctTable,
                    biasPct = biasPct,
                    tID = i + 1
                };

                threadMixedVarsArray[i] = threadMixedVars;
                threads[i] = new Thread(_bulkMixedMTProc);
                threads[i].Start(threadMixedVarsArray[i]);
            }

            for (int i = 0; i < numThreads; i++)
            {
                threads[i].Join();
            }

            for (int i = 0; i < numThreads; i++)
            {
                savedKeysList.AddRange(threadMixedVarsArray[i].savedKeysList);
                threadMixedVarsArray[i].savedKeysList.Clear();
                threadMixedVarsArray[i].savedKeysList = null;
            }

            threadMixedVarsArray = null;

            Console.WriteLine(string.Format("\r\nIteration: {0} Bulk Mixed Complete: {1}, Keys Saved: {2} Memory Delta: {3}", iteration, DateTime.Now, savedKeysList.Count, GC.GetTotalMemory(true) - mem));
        }

        private static void _bulkMixedMTProc(object o)
        {
            _threadMixedVars tVars = (_threadMixedVars)o;

            var bufferBytesSm = new byte[(int)(tVars.pktSz * 0.8)];
            var bufferBytes = new byte[tVars.pktSz];
            var bufferBytesLg = new byte[(int)(tVars.pktSz * 1.2)];
            var million = 1000000;
            var key = new HashKey();

            tVars.barrier.SignalAndWait();

            _startCounting();

            var timeStart = DateTime.Now;
            var localOpsCnt = 0;

            unsafe
            {
                fixed (byte* bufferBytesPtrLg = bufferBytesLg)
                fixed (byte* bufferBytesPtrSm = bufferBytesSm)
                fixed (byte* bufferBytesPtrNm = bufferBytes)
                {
                    for (int i = 1; i <= tVars.keysToCreateCount; )
                    {
                        byte* bufferBytesPtr = bufferBytesPtrNm;

                        int action;
                        int idx = -1;

                        action = tVars.pctTable[localOpsCnt++ % 100];

                        if (action != 'C')
                        {
                            if (tVars.savedKeysList.Count >= 10000 && tVars.biasPct > -1)
                            {
                                int sampleSize = (int)((tVars.biasPct / 100.0) * tVars.savedKeysList.Count);
                                int startIdx = tVars.savedKeysList.Count - sampleSize - 1;
                                idx = sampleSize / ((i % 10) + 1) + startIdx;
                            }
                            else
                            {
                                action = 'C';
                            }
                        }

                        int totalNow = 0;

                        if (action == 'C')
                        {
                            // todo: eliminate random call to speed up
                            key.GenerateRandomKey();

                            *((uint*)(bufferBytesPtr + 0)) = key.uint1;
                            *((uint*)(bufferBytesPtr + 4)) = key.uint2;
                            *((uint*)(bufferBytesPtr + 8)) = key.uint3;
                            *((uint*)(bufferBytesPtr + 12)) = key.uint4;
                            *((uint*)(bufferBytesPtr + 16)) = key.uint5;

                            int version = 69;
                            double before = _stw.ElapsedMilliseconds;
                            var result = tVars.bf.Create(key, bufferBytes, 42, DateTime.MaxValue, version, false);
                            double after = _stw.ElapsedMilliseconds;

                            if (result != Status.Successful)
                            {
                                throw new ApplicationException(string.Format("Unable to create key: {0} [{1}]", key, result));
                            }

                            Interlocked.Increment(ref _itemsCreated);
                            totalNow = Interlocked.Increment(ref _totalOps);

                            Interlocked.Add(ref _createLatency, (long)((after - before) * 1000));

                            if (i % tVars.modSave == 0 && tVars.savedKeysList.Count < tVars.keysToSaveCount)
                            {
                                tVars.savedKeysList.Add(new _keyTracker() { key = key, version = version });
                            }

                            i++;
                        }
                        else if (action == 'R')
                        {
                            byte recordType;
                            DateTime timestamp;
                            int versionNo;
                            byte[] dataBytes;
                            _keyTracker keyTracker;

                            keyTracker = tVars.savedKeysList[idx];

                            double before = _stw.ElapsedMilliseconds;
                            var result = tVars.bf.Read(keyTracker.key, out dataBytes, out recordType, out timestamp, out versionNo);
                            double after = _stw.ElapsedMilliseconds;

                            if (result == Status.KeyFoundButMarkedDeleted)
                            {
                                // deleted, move on
                            }
                            else if (keyTracker.deleted == true && result == Status.Successful)
                            {
                                throw new ApplicationException(string.Format("Not properly deleted key: {0}", keyTracker.key));
                            }
                            else if (versionNo != keyTracker.version && result == Status.Successful)
                            {
                                throw new ApplicationException(string.Format("Not properly updated version key: {0}", keyTracker.key));
                            }
                            else if (result != Status.Successful)
                            {
                                throw new ApplicationException(string.Format("Unable to read key: {0} [{1}]", keyTracker.key, result));
                            }
                            else
                            {
                                unsafe
                                {
                                    fixed (byte* dataBytesPtr = dataBytes)
                                    {
                                        var uint1 = *((uint*)(dataBytesPtr + 0));
                                        var uint2 = *((uint*)(dataBytesPtr + 4));
                                        var uint3 = *((uint*)(dataBytesPtr + 8));
                                        var uint4 = *((uint*)(dataBytesPtr + 12));
                                        var uint5 = *((uint*)(dataBytesPtr + 16));

                                        if (keyTracker.key.uint1 != uint1 || keyTracker.key.uint2 != uint2 || keyTracker.key.uint3 != uint3 || keyTracker.key.uint4 != uint4 || keyTracker.key.uint5 != uint5)
                                        {
                                            throw new ApplicationException("Key mismatch");
                                        }
                                    }
                                }
                            }

                            Interlocked.Increment(ref _itemsRead);
                            totalNow = Interlocked.Increment(ref _totalOps);

                            Interlocked.Add(ref _readLatency, (long)((after - before) * 1000));
                        }
                        else if (action == 'U')
                        {
                            var keyTracker = tVars.savedKeysList[idx];

                            if (keyTracker.deleted == false)
                            {
                                var whichBuffer = keyTracker.key.uint1 % 3;
                                byte[] buffer = bufferBytes;
                                bufferBytesPtr = bufferBytesPtrNm;

                                if (whichBuffer == 1)
                                {
                                    bufferBytesPtr = bufferBytesPtrSm;
                                    buffer = bufferBytesSm;
                                }
                                else if (whichBuffer == 2)
                                {
                                    bufferBytesPtr = bufferBytesPtrLg;
                                    buffer = bufferBytesLg;
                                }

                                *((uint*)(bufferBytesPtr + 0)) = keyTracker.key.uint1;
                                *((uint*)(bufferBytesPtr + 4)) = keyTracker.key.uint2;
                                *((uint*)(bufferBytesPtr + 8)) = keyTracker.key.uint3;
                                *((uint*)(bufferBytesPtr + 12)) = keyTracker.key.uint4;
                                *((uint*)(bufferBytesPtr + 16)) = keyTracker.key.uint5;

                                var timestamp = DateTime.MaxValue;
                                double before = _stw.ElapsedMilliseconds;
                                var result = tVars.bf.Update(keyTracker.key, buffer, 42, ref timestamp, ref keyTracker.version, false);
                                double after = _stw.ElapsedMilliseconds;

                                if (result != Status.Successful)
                                {
                                    throw new ApplicationException(string.Format("Unable to update key: {0} [{1}]", keyTracker.key, result));
                                }

                                Interlocked.Increment(ref _itemsUpdated);
                                totalNow = Interlocked.Increment(ref _totalOps);

                                Interlocked.Add(ref _updateLatency, (long)((after - before) * 1000));
                            }
                        }
                        else // action == 'D'
                        {
                            var keyTracker = tVars.savedKeysList[idx];

                            if (keyTracker.deleted == false)
                            {
                                double before = _stw.ElapsedMilliseconds;
                                var result = tVars.bf.Delete(keyTracker.key, false);
                                double after = _stw.ElapsedMilliseconds;

                                if (result == Status.KeyFoundButMarkedDeleted)
                                {
                                    throw new ApplicationException(string.Format("Not properly marked deleted key: {0}", keyTracker.key));
                                }
                                else if (result != Status.Successful)
                                {
                                    throw new ApplicationException(string.Format("Unable to delete key: {0} [{1}]", keyTracker.key, result));
                                }

                                keyTracker.deleted = true;

                                Interlocked.Increment(ref _itemsDeleted);
                                totalNow = Interlocked.Increment(ref _totalOps);

                                Interlocked.Add(ref _deleteLatency, (long)((after - before) * 1000));
                            }
                        }

                        if (totalNow > 0 && totalNow % million == 0)
                        {
                            double secs = _stw.ElapsedMilliseconds / 1000.0;

                            Console.WriteLine(string.Format("\r\nIteration: {0} Bulk Mixed Test Stats [CRUD: {10} CRUDtps: {1:0} Ctps: {2:0} ({3:0}µs) Rtps: {4:0} ({5:0}µs) Utps: {6:0} ({7:0}µs) Dtps: {8:0} ({9:0}µs)]",
                                tVars.iteration, _totalOps / secs, _itemsCreated / secs, _createLatency / _itemsCreated, _itemsRead / secs, _readLatency / _itemsRead,
                                    _itemsUpdated / secs, _updateLatency / _itemsUpdated, _itemsDeleted / secs, _deleteLatency / _itemsDeleted, _totalOps));

                            for (int tid = 0; tid < _doStats.Length; tid++)
                            {
                                _doStats[tid] = true;
                            }
                        }

                        if (_doStats[tVars.tID - 1] == true)
                        {
                            _doStats[tVars.tID - 1] = false;

                            Console.WriteLine(string.Format("Iteration: {0} tID: {7} Reported Stats [C: {1} R: {2} U: {3} D: {4} FRP: {5:0.00} XBY: {6}]",
                                tVars.iteration, tVars.bf.ItemsCreated, tVars.bf.ItemsRead, tVars.bf.ItemsUpdated, tVars.bf.ItemsDeleted,
                                    tVars.bf.FragmentationPercent, tVars.bf.ItemBytesExtant, tVars.tID));

                            //File.AppendAllText("performance.log", string.Format("{0},{1:0},{2:0},{3:0},{4:0}\r\n", _itemsCreated, _totalOps / secs, _itemsCreated / secs , _itemsRead / secs, _itemsUpdated / secs));
                        }
                    }
                }
            }
        }

        private static BloomFile _loadBloomFile(string filePath, int iteration)
        {
            DateTime timeStart = DateTime.Now;
            long mem = GC.GetTotalMemory(true);

            Console.WriteLine(string.Format("Iteration: {0} Loading Start: {1}", iteration, DateTime.Now));

            var bf = BloomFile.LoadBloomFile(filePath);

            bf.OnFlushingStart += _startFlushing;
            bf.OnFlushingStop += _stopFlushing;

            Console.WriteLine(string.Format("Iteration: {0} Loading Complete: {1} Memory Delta: {2}", iteration, DateTime.Now, GC.GetTotalMemory(true) - mem));

            return bf;
        }

        private static void _bulkReadKeysFromSavedKeysList(BloomFile bf, int iteration, List<HashKey> savedKeyList)
        {
            int tenk = 10000;
            var timeStart = DateTime.Now;

            Console.WriteLine(string.Format("Iteration: {0} Bulk Reads Start: {1}", iteration, timeStart));

            var keysSavedCount = savedKeyList.Count;

            for (int i = 1; i <= keysSavedCount; i++)
            {
                byte recordType;
                DateTime timestamp;
                int versionNo;
                byte[] dataBytes;

                var key = savedKeyList[i - 1];

                var result = bf.Read(key, out dataBytes, out recordType, out timestamp, out versionNo);

                if (result != Status.Successful)
                {
                    throw new ApplicationException(string.Format("Unable to read key: {0} [{1}]", key, result));
                }

                unsafe
                {
                    fixed (byte* dataBytesPtr = dataBytes)
                    {
                        var uint1 = *((uint*)(dataBytesPtr + 0));
                        var uint2 = *((uint*)(dataBytesPtr + 4));
                        var uint3 = *((uint*)(dataBytesPtr + 8));
                        var uint4 = *((uint*)(dataBytesPtr + 12));
                        var uint5 = *((uint*)(dataBytesPtr + 16));

                        if (key.uint1 != uint1 || key.uint2 != uint2 || key.uint3 != uint3 || key.uint4 != uint4 || key.uint5 != uint5)
                        {
                            throw new ApplicationException("Key mismatch");
                        }
                    }
                }

                if (i % tenk == 0)
                {
                    Console.WriteLine(string.Format("Iteration: {0} Read: {1} Time Delta: {2}", iteration, i, DateTime.Now - timeStart));
                }
            }

#if TRACKFP
            Console.WriteLine(string.Format("Iteration: {0} Bulk Reads Complete: {1} Time Delta: {2} False: {2}", iteration, keysSavedCount, DateTime.Now - timeStart, bf.FalsePositives()));
#else
            Console.WriteLine(string.Format("Iteration: {0} Bulk Reads Complete: {1} Time Delta: {2}", iteration, keysSavedCount, DateTime.Now - timeStart));
#endif
        }

        private static void _bulkReadKeysMTFromSavedKeysList(BloomFile bf, int iteration, int numThreads, int keysToRead, List<_keyTracker> savedKeyList)
        {
            var timeStart = DateTime.Now;

            Console.WriteLine(string.Format("Iteration: {0} Bulk Reads MT Start: {1}", iteration, timeStart));

            var threads = new Thread[numThreads];
            _threadReadVars[] threadReadVarsArray = new _threadReadVars[numThreads];
            Barrier barrier = new Barrier(numThreads);

            for (int i = 0; i < numThreads; i++)
            {
                var threadReadVars = new _threadReadVars
                {
                    bf = bf,
                    iteration = iteration,
                    barrier = barrier,
                    tID = i + 1
                };

                threadReadVarsArray[i] = threadReadVars;
            }

            int threadKeysToReadCount = keysToRead / numThreads;

            for (int i = 0; i < numThreads; i++)
            {
                threadReadVarsArray[i].savedKeysList = savedKeyList.GetRange(0, threadKeysToReadCount);
                savedKeyList.RemoveRange(0, threadKeysToReadCount);
            }

            for (int i = 0; i < numThreads; i++)
            {
                threads[i] = new Thread(_bulkReadsMTProc);
                threads[i].Start(threadReadVarsArray[i]);
            }

            for (int i = 0; i < numThreads; i++)
            {
                threads[i].Join();
            }

            for (int i = 0; i < numThreads; i++)
            {
                threadReadVarsArray[i].savedKeysList.Clear();
                threadReadVarsArray[i].savedKeysList = null;
            }

            threadReadVarsArray = null;

#if TRACKFP
            Console.WriteLine(string.Format("Iteration: {0} Bulk Reads MT Complete: {1} Time Delta: {2} False: {2}", iteration, _itemsRead, DateTime.Now - timeStart, bf.FalsePositives()));
#else
            Console.WriteLine(string.Format("Iteration: {0} Bulk Reads MT Complete: {1} Time Delta: {2}", iteration, _itemsRead, DateTime.Now - timeStart));
#endif
        }

        private static void _bulkReadsMTProc(object o)
        {
            _threadReadVars tVars = (_threadReadVars)o;

            int hundk = 100000;

            tVars.barrier.SignalAndWait();

            var timeStart = DateTime.Now;

            var keysSavedCount = tVars.savedKeysList.Count;

            for (int i = 1; i <= keysSavedCount; i++)
            {
                byte recordType;
                DateTime timestamp;
                int versionNo;
                byte[] dataBytes;

                var keyTracker = tVars.savedKeysList[i - 1];

                var result = tVars.bf.Read(keyTracker.key, out dataBytes, out recordType, out timestamp, out versionNo);

                if (result == Status.KeyFoundButMarkedDeleted)
                {
                    // deleted, move on
                }
                else if (keyTracker.deleted == true && result == Status.Successful)
                {
                    throw new ApplicationException(string.Format("Not properly deleted key: {0}", keyTracker.key));
                }
                else if (versionNo != keyTracker.version && result == Status.Successful)
                {
                    throw new ApplicationException(string.Format("Not properly updated version key: {0}", keyTracker.key));
                }
                else if (result != Status.Successful)
                {
                    throw new ApplicationException(string.Format("Unable to read key: {0} [{1}]", keyTracker.key, result));
                }
                else
                {
                    unsafe
                    {
                        fixed (byte* dataBytesPtr = dataBytes)
                        {
                            var uint1 = *((uint*)(dataBytesPtr + 0));
                            var uint2 = *((uint*)(dataBytesPtr + 4));
                            var uint3 = *((uint*)(dataBytesPtr + 8));
                            var uint4 = *((uint*)(dataBytesPtr + 12));
                            var uint5 = *((uint*)(dataBytesPtr + 16));

                            if (keyTracker.key.uint1 != uint1 || keyTracker.key.uint2 != uint2 || keyTracker.key.uint3 != uint3 || keyTracker.key.uint4 != uint4 || keyTracker.key.uint5 != uint5)
                            {
                                throw new ApplicationException("Key mismatch");
                            }
                        }
                    }
                }

                var readNow = Interlocked.Increment(ref _itemsRead);

                if (readNow % hundk == 0)
                {
                    Console.WriteLine(string.Format("Iteration: {0} Read: {1} Time Delta: {2}", tVars.iteration, readNow, DateTime.Now - timeStart));
                }
            }
        }

        #endregion
    }
}
