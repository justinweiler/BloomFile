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
using System.Threading;

namespace Phrenologix
{
    public delegate void FlushingEventHandler(object sender, EventArgs e);

    public class BloomFile : IDisposable
    {
        private const int _numTrees = 8;
        private settings _settings = new settings();
        private bloomFileTree[] _bloomFileTrees = new bloomFileTree[_numTrees];
        private Timer _flushTimer;
        private object _flushLock = new object();
        private bool _isDisposed;

        public event FlushingEventHandler OnFlushingStart;
        public event FlushingEventHandler OnFlushingStop;

        public static int FlushInterval = 10000;

        private BloomFile()
        {
        }

        ~BloomFile()
        {
            Dispose(false);
        }

        public static BloomFile LoadBloomFile(string filePath)
        {
            var bloomFile = new BloomFile();

            bloomFile._settings.loadSettings(filePath);

            for (int i = 0; i < _numTrees; i++)
            {
                var bloomFileTree = new bloomFileTree(i, string.Format("{0}{1}", filePath, i), bloomFile._settings, true);
                bloomFile._bloomFileTrees[i] = bloomFileTree;
            }

            bloomFile._flushTimer = new Timer(_flushAllBloomFileTrees, bloomFile, FlushInterval, Timeout.Infinite);

            return bloomFile;
        }

        public static BloomFile CreateBloomFile(string filePath, int averageDataItemSize, float averageDataItemSizeSlack, bool doComputeChecksum)
        {
            int blockStartPosition;
            long fileIncr = ((long)ioConstants.computeTotalBufferSize(96, averageDataItemSize, averageDataItemSizeSlack, out blockStartPosition) * 4L * 8L * 16L * 32L * 64L) / 80L;

            var bloomFile = new BloomFile();

            bloomFile._settings.blossomKeyCapacity = 95.367f;
            bloomFile._settings.errorRate = 0.02f;
            bloomFile._settings.padFactor = 1.0f;
            bloomFile._settings.averageDataItemSize = averageDataItemSize;
            bloomFile._settings.averageDataItemSizeSlack = averageDataItemSizeSlack;
            bloomFile._settings.doComputeChecksum = doComputeChecksum;
            bloomFile._settings.branchFactors = new byte[] { 4, 8, 16, 32, 64 };
            bloomFile._settings.initialFileSize = fileIncr;
            bloomFile._settings.growFileSize = fileIncr;

            bloomFile._settings.saveSettings(filePath);

            for (int i = 0; i < _numTrees; i++)
            {
                var bloomFileTree = new bloomFileTree(i, string.Format("{0}{1}", filePath, i), bloomFile._settings, false);
                bloomFile._bloomFileTrees[i] = bloomFileTree;
            }

            bloomFile._flushTimer = new Timer(_flushAllBloomFileTrees, bloomFile, FlushInterval, Timeout.Infinite);

            return bloomFile;
        }

#if TRACKFP
        public int FalsePositives()
        {
            int falsePositives = 0;

            for (int i = 0; i < _numTrees; i++)
            {
                var bfTree = _bfTrees[i];
                falsePositives += bfTree.falsePositives();
            }

            return falsePositives;
        }
#endif

        public long ItemsCreated
        {
            get
            {
                return bloomFileTree.itemsCreated - bloomFileTree.itemsUpdatedCreated;
            }
        }

        public long ItemsRead
        {
            get
            {
                return bloomFileTree.itemsRead;
            }
        }

        public long ItemsUpdated
        {
            get
            {
                return bloomFileTree.itemsUpdatedCreated + bloomFileTree.itemsUpdatedInPlace;
            }
        }

        public long ItemsDeleted
        {
            get
            {
                return bloomFileTree.itemsDeleted;
            }
        }

        public long ItemBytesExtant
        {
            get
            {
                return bloomFileTree.itemBytesExtant;
            }
        }

        public double FragmentationPercent
        {
            get
            {
                if (bloomFileTree.itemsCreated > 0)
                {
                    return ((bloomFileTree.itemsDeleted + bloomFileTree.itemsUpdatedCreated) * 100.0) / bloomFileTree.itemsCreated;
                }

                return double.NaN;
            }
        }

        public Status Create(HashKey key, byte[] dataBytes, byte recordType, DateTime timestamp, int versionNo, bool flush = false)
        {
            if (dataBytes == null || dataBytes.Length == 0)
            {
                return Status.BadParameter;
            }
            else if (versionNo < 0)
            {
                return Status.BadParameter;
            }

            var i = ((key.uint5 & 0x70000000) >> 28);
            return _bloomFileTrees[i].createBloomFile(key, dataBytes, recordType, timestamp, versionNo, _settings.averageDataItemSizeSlack, flush);
        }

        public Status Read(HashKey key, out byte[] dataBytes, out byte recordType, out DateTime timestamp, out int versionNo)
        {
            var i = ((key.uint5 & 0x70000000) >> 28);
            return _bloomFileTrees[i].readBloomFile(key, out dataBytes, out recordType, out timestamp, out versionNo);
        }

        public Status Update(HashKey key, byte[] dataBytes, byte recordType, ref DateTime timestamp, ref int versionNo, bool flush = false)
        {
            if (dataBytes == null || dataBytes.Length == 0)
            {
                return Status.BadParameter;
            }
            else if (versionNo < 0)
            {
                return Status.BadParameter;
            }

            var i = ((key.uint5 & 0x70000000) >> 28);
            return _bloomFileTrees[i].updateBloomFile(key, dataBytes, recordType, ref timestamp, ref versionNo, _settings.averageDataItemSizeSlack, flush);
        }

        public Status Delete(HashKey key, bool flush = false)
        {
            var i = ((key.uint5 & 0x70000000) >> 28);
            return _bloomFileTrees[i].deleteBloomFile(key, flush);
        }

        public void Flush()
        {
            _flushAllBloomFileTrees(this);
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        private static void _flushAllBloomFileTrees(object state)
        {
            var bloomFile = (BloomFile)state;

            if (bloomFile._isDisposed == false && bloomFile._flushTimer != null)
            {
                lock (bloomFile._flushLock)
                {
                    if (bloomFile._isDisposed == false && bloomFile._flushTimer != null)
                    {
                        var now = DateTime.Now;

                        var bloomFileTrees = bloomFile._bloomFileTrees;

                        if (bloomFileTrees != null)
                        {
                            if (bloomFile.OnFlushingStart != null)
                            {
                                ThreadPool.QueueUserWorkItem(delegate(object o) { bloomFile.OnFlushingStart(bloomFile, null); });
                            }

                            for (int i = 0; i < 8; i++)
                            {
                                if (bloomFileTrees[i] != null)
                                {
                                    bloomFileTrees[i].flushCurrentBloomFileBlossom();
                                    bloomFileTrees[i].flushAllBloomFileBranches(now);
                                }
                            }

                            if (bloomFile.OnFlushingStop != null)
                            {
                                ThreadPool.QueueUserWorkItem(delegate(object o) { bloomFile.OnFlushingStop(bloomFile, null); });
                            }
                        }

                        if (bloomFile._flushTimer != null)
                        {
                            bloomFile._flushTimer.Change(FlushInterval, Timeout.Infinite);
                        }
                    }
                }
            }
        }

        protected virtual void Dispose(bool disposing)
        {
            if (_isDisposed == false)
            {
                lock (_flushLock)
                {
                    if (_isDisposed == false)
                    {
                        _flushAllBloomFileTrees(this);

                        _isDisposed = true;

                        if (_flushTimer != null)
                        {
                            _flushTimer.Change(Timeout.Infinite, Timeout.Infinite);
                            _flushTimer.Dispose();
                            _flushTimer = null;
                        }

                        if (_bloomFileTrees != null)
                        {
                            for (int i = 0; i < 8; i++)
                            {
                                _bloomFileTrees[i].Dispose();
                                _bloomFileTrees[i] = null;
                            }

                            _bloomFileTrees = null;
                        }
                    }
                }
            }
        }
    }
}
