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
using System.IO;
using System.Threading;
using System.Xml;

namespace Phrenologix
{
    internal class bloomFileTree : IDisposable
    {
        private string _filePath;
        private settings _settings;
        private int _blossomIDCounter;
        private int[] _branchKeyCapacities;
        private fileStorage _blossomFileStorage;
        private fileStorage _branchFileStorage;
        private List<bloomFileBranch> _rootList = new List<bloomFileBranch>();
        private bloomFileBlossom _currentBloomFileBlossom;
        private blossomReaderWriter _blossomCreator;
        private blossomReaderWriter _blossomReader;
        private blossomReaderWriter _blossomUpdater;
        private blossomReaderWriter _blossomDeleter;
        private branchReaderWriter _branchReaderWriter;
        private int _treeID;
        private static long _itemsCreated;
        private static long _itemsRead;
        private static long _itemsUpdatedInPlace;
        private static long _itemsUpdatedCreated;
        private static long _itemsDeleted;
        private static long _itemBytesExtant;

        internal int treeID
        {
            get
            {
                return _treeID;
            }
        }

        internal static long itemsCreated
        {
            get
            {
                return _itemsCreated;
            }
            set
            {
                _itemsCreated = value;
            }
        }

        internal static long itemsRead
        {
            get
            {
                return _itemsRead;
            }
            set
            {
                _itemsRead = value;
            }
        }

        internal static long itemsUpdatedInPlace
        {
            get
            {
                return _itemsUpdatedInPlace;
            }
            set
            {
                _itemsUpdatedInPlace = value;
            }
        }

        internal static long itemsUpdatedCreated
        {
            get
            {
                return _itemsUpdatedCreated;
            }
            set
            {
                _itemsUpdatedCreated = value;
            }
        }

        internal static long itemsDeleted
        {
            get
            {
                return _itemsDeleted;
            }
            set
            {
                _itemsDeleted = value;
            }
        }

        internal static long itemBytesExtant
        {
            get
            {
                return _itemBytesExtant;
            }
            set
            {
                _itemBytesExtant = value;
            }
        }

        public void Dispose()
        {
            _currentBloomFileBlossom.flushCreator(_blossomCreator);

            _branchFileStorage.Dispose();
            _blossomFileStorage.Dispose();

            _currentBloomFileBlossom = null;
            _blossomCreator = null;
            _blossomReader = null;
            _blossomUpdater = null;
            _blossomDeleter = null;
            _blossomFileStorage = null;
            _branchReaderWriter = null;
            _branchFileStorage = null;
        }

        internal bloomFileTree(int treeID, string filePath, settings settings, bool reconstruct)
        {
            _treeID = treeID;
            _filePath = filePath;
            _settings = settings;

            // setup compute checksum
            blossomReaderWriter.doComputeChecksum = _settings.doComputeChecksum;
            branchReaderWriter.doComputeChecksum = _settings.doComputeChecksum;

            // recopy branch factors into a more usable local array in bloomFileBranch
            var treeBranchFactors = new byte[_settings.branchFactors.Length + 1];
            Array.Copy(_settings.branchFactors, 0, treeBranchFactors, 1, _settings.branchFactors.Length);
            bloomFileBranch.branchFactors = treeBranchFactors;

            // compute levels and branch key capacities, branchKeyCapacities level 0 is ACTUAL int key capacity
            var levels = _settings.branchFactors.Length;
            _branchKeyCapacities = new int[levels + 1];
            _branchKeyCapacities[0] = (int)Math.Ceiling(_settings.blossomKeyCapacity);

            for (byte level = 1; level <= levels; level++)
            {
                // use pad factor to enhance accuracy of filter
                var keyCapacity = _settings.padFactor * _settings.blossomKeyCapacity;

                // compute branch cost for level 2 and above
                for (byte i = 2; i <= level; i++)
                {
                    keyCapacity *= _settings.branchFactors[i - 2];
                }

                _branchKeyCapacities[level] = (int)Math.Ceiling(keyCapacity);
            }

            // compute buffer with ACTUAL int key capacity for exact RAM size
            int bloomFileBlockStartPosition;
            int totalBufferSize = ioConstants.computeTotalBufferSize(_branchKeyCapacities[0], _settings.averageDataItemSize, _settings.averageDataItemSizeSlack, out bloomFileBlockStartPosition);

            // setup blossom storage, it needs the ACTUAL int key capacity and real buffer size
            _blossomFileStorage = new fileStorage(_treeID, filePath + ".blossom", totalBufferSize, _settings.initialFileSize, _settings.growFileSize);

            // create blossom IO helpers
            _blossomCreator = new blossomReaderWriter(_blossomFileStorage, _branchKeyCapacities[0], totalBufferSize, bloomFileBlockStartPosition, true);
            _blossomReader = new blossomReaderWriter(_blossomFileStorage, _branchKeyCapacities[0], totalBufferSize, bloomFileBlockStartPosition, false);
            _blossomUpdater = _blossomReader;
            _blossomDeleter = _blossomReader;

            // setup branch storage
            var branchSize = 128 * 1024 * 1024;
            _branchFileStorage = new fileStorage(_treeID, _filePath + ".branch", 0, branchSize, branchSize);
            _branchReaderWriter = new branchReaderWriter(_branchFileStorage);

            if (reconstruct == false)
            {
                // create current blossom
                _currentBloomFileBlossom = new bloomFileBlossom(++_blossomIDCounter, _blossomFileStorage.firstFileBlockPosition, _branchKeyCapacities[1], _settings.errorRate);

                // create first branching leg of the tree
                bloomFileBranch child = _currentBloomFileBlossom;

                for (byte level = 2; level <= levels; level++)
                {
                    // create new branch and compute key capacity by using Level N branch costs * blossom base key capacity
                    var parent = new bloomFileBranch(0, level, _branchKeyCapacities[level], _settings.errorRate);
                    parent.addChildBranch(child);
                    child = parent;
                }

                _rootList.Add(child);
                _blossomCreator.reset(_currentBloomFileBlossom.blossomID);
            }
            else
            {
                _reconstructBloomFileBranches();
                _blossomCreator.restoreFromFile(_currentBloomFileBlossom.blossomFilePosition, _currentBloomFileBlossom.blossomID);
            }
        }

        internal Status createBloomFile(HashKey key, byte[] dataBytes, byte recordType, DateTime timestamp, int versionNo, float slackFactor, bool flush)
        {
            int tries = 2;
            Status created = Status.Unsuccessful;

            lock (this)
            {
                while (tries-- > 0)
                {
                    created = _currentBloomFileBlossom.createBloomFileToCreator(_blossomCreator, key, dataBytes, recordType, timestamp, versionNo, slackFactor, flush);

                    // add a blossom if needed
                    if (created == Status.Unsuccessful && tries > 0)
                    {
                        _currentBloomFileBlossom.flushCreator(_blossomCreator);

                        var parent = _currentBloomFileBlossom.parent;

                        if (parent.branchCount == _settings.branchFactors[0])
                        {
                            parent = _expandBloomFileTree(_currentBloomFileBlossom);
                        }

                        // use new blossom now
                        _currentBloomFileBlossom = new bloomFileBlossom(++_blossomIDCounter, _blossomFileStorage.advanceNextFileBlockPosition(_blossomCreator.computeBlossomFileSize()), _branchKeyCapacities[1], _settings.errorRate);
                        _blossomCreator.reset(_currentBloomFileBlossom.blossomID);
                        parent.addChildBranch(_currentBloomFileBlossom);
                    }
                    else
                    {
                        break;
                    }
                }
            }

            if (created == Status.Successful)
            {
                Interlocked.Increment(ref _itemsCreated);
                Interlocked.Add(ref _itemBytesExtant, dataBytes.Length);
            }

            return created;
        }

        internal Status readBloomFile(HashKey key, out byte[] dataBytes, out byte recordType, out DateTime timestamp, out int versionNo)
        {
            // setup defaults
            recordType = 0;
            timestamp = DateTime.MinValue;
            versionNo = 0;
            dataBytes = null;
            Status read = Status.KeyNotFound;

            lock (this)
            {
                if (_currentBloomFileBlossom.containsKey(key, _blossomCreator) == true)
                {
                    read = _currentBloomFileBlossom.readBloomFileFromCreator(_blossomCreator, key, out dataBytes, out recordType, out timestamp, out versionNo);

                    // inconclusive result, something is very wrong
                    if (read == Status.KeyNotFound)
                    {
                        throw new InvalidDataException("Unexpected KeyNotFound return value in readBloomFileFromCreator");
                    }
                }
                else
                {
                    byte evalRecordType = 0;
                    DateTime evalTimestamp = DateTime.MinValue;
                    int evalVersionNo = 0;
                    byte[] evalDataBytes = null;

                    Func<bloomFileBlossom, Status> evaluator = delegate(bloomFileBlossom bloomFileBlossom)
                    {
                        if (bloomFileBlossom != _currentBloomFileBlossom)
                        {
                            return bloomFileBlossom.readBloomFileFromReader(_blossomReader, key, out evalDataBytes, out evalRecordType, out evalTimestamp, out evalVersionNo);
                        }

                        // keep looking
                        return Status.KeyNotFound;
                    };

                    for (int i = _rootList.Count - 1; i >= 0; i--)
                    {
                        read = _rootList[i].superContainsKey(key, _blossomReader, evaluator);

                        // a KeyNotFound result is inconclusive
                        if (read != Status.KeyNotFound)
                        {
                            recordType = evalRecordType;
                            timestamp = evalTimestamp;
                            versionNo = evalVersionNo;
                            dataBytes = evalDataBytes;
                            break;
                        }
                    }
                }
            }

            if (read == Status.Successful || read == Status.KeyFoundButMarkedDeleted)
            {
                Interlocked.Increment(ref _itemsRead);
            }

            return read;
        }

        internal Status updateBloomFile(HashKey key, byte[] dataBytes, byte recordType, ref DateTime timestamp, ref int versionNo, float slackFactor, bool flush)
        {
            Status updated = Status.KeyNotFound;

            lock (this)
            {
                // either utilize current blossom or locate record in old blossoms
                if (_currentBloomFileBlossom.containsKey(key, _blossomCreator) == true)
                {
                    updated = _currentBloomFileBlossom.updateBloomFileToCreator(_blossomCreator, key, dataBytes, recordType, ref timestamp, ref versionNo, flush);

                    // inconclusive result, something is very wrong
                    if (updated == Status.KeyNotFound)
                    {
                        throw new InvalidDataException("Unexpected KeyNotFound return value in updateBloomFileToCreator");
                    }
                }
                else
                {
                    DateTime evalTimestamp = timestamp;
                    int evalVersionNo = versionNo;

                    Func<bloomFileBlossom, Status> evaluator = delegate(bloomFileBlossom bloomFileBlossom)
                    {
                        if (bloomFileBlossom != _currentBloomFileBlossom)
                        {
                            return bloomFileBlossom.updateBloomFileToUpdater(_blossomUpdater, key, dataBytes, recordType, ref evalTimestamp, ref evalVersionNo, flush);
                        }

                        // keep looking
                        return Status.KeyNotFound;
                    };

                    for (int i = _rootList.Count - 1; i >= 0; i--)
                    {
                        updated = _rootList[i].superContainsKey(key, _blossomUpdater, evaluator);

                        // a KeyNotFound result is inconclusive
                        if (updated != Status.KeyNotFound)
                        {
                            timestamp = evalTimestamp;
                            versionNo = evalVersionNo;
                            break;
                        }
                    }
                }

                // did not find or could not update, force a normal write
                if (updated != Status.Successful)
                {
                    updated = createBloomFile(key, dataBytes, recordType, timestamp, versionNo, slackFactor, flush);

                    if (updated == Status.Successful)
                    {
                        Interlocked.Increment(ref _itemsUpdatedCreated);
                    }
                }
                else
                {
                    Interlocked.Increment(ref _itemsUpdatedInPlace);
                }
            }

            return updated;
        }

        internal Status deleteBloomFile(HashKey key, bool flush)
        {
            Status deleted = Status.KeyNotFound;

            lock (this)
            {
                // either utilize current blossom or locate record in old blossoms
                if (_currentBloomFileBlossom.containsKey(key, _blossomCreator) == true)
                {
                    deleted = _currentBloomFileBlossom.deleteBloomFileToCreator(_blossomCreator, key, flush);

                    // inconclusive result, something is very wrong
                    if (deleted == Status.KeyNotFound)
                    {
                        throw new InvalidDataException("Unexpected KeyNotFound return value in deleteBloomFileToCreator");
                    }
                }
                else
                {
                    Func<bloomFileBlossom, Status> evaluator = delegate(bloomFileBlossom bloomFileBlossom)
                    {
                        if (bloomFileBlossom != _currentBloomFileBlossom)
                        {
                            return bloomFileBlossom.deleteBloomFileToDeleter(_blossomDeleter, key, flush);
                        }

                        // keep looking
                        return Status.KeyNotFound;
                    };

                    for (int i = _rootList.Count - 1; i >= 0; i--)
                    {
                        deleted = _rootList[i].superContainsKey(key, _blossomDeleter, evaluator);

                        // a KeyNotFound result is inconclusive
                        if (deleted != Status.KeyNotFound)
                        {
                            break;
                        }
                    }
                }
            }

            if (deleted == Status.Successful)
            {
                Interlocked.Increment(ref _itemsDeleted);
            }

            return deleted;
        }

        internal void flushAllBloomFileBranches(DateTime timeOfFlush)
        {
            var rootCount = _rootList.Count;

            for (int i = 0; i < rootCount; i++)
            {
                var lastBranchFilePosition = _branchFileStorage.lastFileBlockPosition;

                lock (_rootList[i])
                {
                    _rootList[i].flushBloomFileBranch(_branchReaderWriter, ref lastBranchFilePosition, timeOfFlush);
                }
            }

            _branchFileStorage.flush();
        }

        internal void flushCurrentBloomFileBlossom()
        {
            lock (this)
            {
                _currentBloomFileBlossom.flushCreator(_blossomCreator);

            }
        }

#if TRACKFP
        internal int falsePositives()
        {
            int total = 0;

            foreach (var branch in _root)
            {
                total += branch.countFalsePositives(1);
            }

            return total;
        }
#endif

        // rebuild tree upon load
        private void _reconstructBloomFileBranches()
        {
            bloomFileBranch parent = null;

            while (true)
            {
                long branchFilePosition;
                byte level;
                DateTime timestamp;
                long blossomFilePosition;
                int blossomID;

                var bytes = _branchReaderWriter.readBranchFromFile(out branchFilePosition, out level, out timestamp, out blossomFilePosition, out blossomID);

                // stop on EOF
                if (bytes == null)
                {
                    break;
                }

                _branchReaderWriter.advanceNextBranchFilePosition(ioConstants.branchOverhead + bytes.Length);

                if (level > 1)
                {
                    var branch = new bloomFileBranch(branchFilePosition, level, _branchKeyCapacities[level], _settings.errorRate, bytes);

                    if (_rootList.Count == 0 || _rootList[0].level == level)
                    {
                        _rootList.Add(branch);
                        parent = branch;
                    }
                    else
                    {
                        while (parent.level <= branch.level)
                        {
                            parent = parent.parent;
                        }

                        parent.addChildBranch(branch);
                        parent = branch;
                    }
                }
                else
                {
                    var blossom = new bloomFileBlossom(blossomID, blossomFilePosition, _branchKeyCapacities[1], _settings.errorRate, bytes, branchFilePosition);
                    parent.addChildBranch(blossom);
                    _currentBloomFileBlossom = blossom;
                }
            }
        }

        // expand the tree as we grow
        private bloomFileBranch _expandBloomFileTree(bloomFileBlossom storage)
        {
            bloomFileBranch position = storage;

            while (true)
            {
                position = position.parent;
                byte level = (byte)(position != null ? position.level - 1 : _rootList[0].level);

                if (position == null || position.branchCount < bloomFileBranch.branchFactors[level])
                {
                    while (level > 1)
                    {
                        // create new child branch
                        var childBranch = new bloomFileBranch(0, level, _branchKeyCapacities[level], _settings.errorRate);

                        if (position == null)
                        {
                            _rootList.Add(childBranch);
                        }
                        else
                        {
                            position.addChildBranch(childBranch);
                        }

                        position = childBranch;

                        level--;
                    }

                    return position;
                }
            }
        }
    }
}
