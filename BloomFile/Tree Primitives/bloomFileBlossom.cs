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

namespace Phrenologix
{
    internal class bloomFileBlossom : bloomFileBranch
    {
        private long _blossomFilePosition;

        internal long blossomFilePosition
        {
            get
            {
                return _blossomFilePosition;
            }
        }

#if BLOSSOMID
        private int  _blossomID;

        internal int blossomID
        {
            get
            {
                return _blossomID;
            }
            set
            {
                _blossomID = value;
            }
        }
#else
        internal int blossomID
        {
            get
            {
                return 0;
            }
            private set
            {
            }
        }
#endif

        internal bloomFileBlossom(int blossomID, long blossomFilePosition, int keyCapacity, float errorRate, byte[] hashBits = null, long branchFilePosition = 0) :
            base(branchFilePosition, 1, keyCapacity, errorRate, hashBits)
        {
            this.blossomID = blossomID;
            _blossomFilePosition = blossomFilePosition;
        }

        internal Status createBloomFileToCreator(blossomReaderWriter blossomCreator, HashKey key, byte[] dataBytes, byte recordType, DateTime timestamp, int versionNo, float slackFactor, bool flush)
        {
            if (blossomCreator.canCreate(dataBytes) == false)
            {
                return Status.Unsuccessful;
            }

            lock (_getRoot())
            {
                blossomCreator.createBloomFileToBuffer(key, dataBytes, recordType, timestamp, versionNo, slackFactor);

                superAddKey(key);

                dirty |= 0x01;

                if (flush == true)
                {
                    flushCreator(blossomCreator);
                }
            }

            return Status.Successful;
        }

        internal Status readBloomFileFromCreator(blossomReaderWriter blossomCreator, HashKey key, out byte[] dataBytes, out byte recordType, out DateTime timestamp, out int versionNo)
        {
            return blossomCreator.readBloomFileFromBuffer(key, out dataBytes, out recordType, out timestamp, out versionNo);
        }

        internal Status readBloomFileFromReader(blossomReaderWriter blossomReader, HashKey key, out byte[] dataBytes, out byte recordType, out DateTime timestamp, out int versionNo)
        {
            return blossomReader.readBloomFileFromFile(_blossomFilePosition, blossomID, key, out dataBytes, out recordType, out timestamp, out versionNo);
        }

        internal Status updateBloomFileToCreator(blossomReaderWriter blossomCreator, HashKey key, byte[] dataBytes, byte recordType, ref DateTime timestamp, ref int versionNo, bool flush)
        {
            lock (_getRoot())
            {
                var updated = blossomCreator.updateBloomFileToBuffer(key, dataBytes, recordType, ref timestamp, ref versionNo);

                if (updated == Status.Successful)
                {
                    superAddKey(key);

                    dirty |= 0x01;

                    if (flush == true)
                    {
                        flushCreator(blossomCreator);
                    }
                }

                return updated;
            }
        }

        internal Status updateBloomFileToUpdater(blossomReaderWriter blossomUpdater, HashKey key, byte[] dataBytes, byte recordType, ref DateTime timestamp, ref int versionNo, bool flush)
        {
            lock (_getRoot())
            {
                var updated = blossomUpdater.updateBloomFileToFile(_blossomFilePosition, blossomID, key, dataBytes, recordType, ref timestamp, ref versionNo);

                if (updated == Status.Successful)
                {
                    superAddKey(key);

                    if (flush == true)
                    {
                        blossomUpdater.flush();
                    }
                }

                return updated;
            }
        }

        internal Status deleteBloomFileToCreator(blossomReaderWriter blossomCreator, HashKey key, bool flush)
        {
            lock (_getRoot())
            {
                var deleted = blossomCreator.deleteBloomFileToBuffer(key);

                if (deleted == Status.Successful)
                {
                    superAddKey(key);

                    dirty |= 0x01;

                    if (flush == true)
                    {
                        flushCreator(blossomCreator);
                    }
                }

                return deleted;
            }
        }

        internal Status deleteBloomFileToDeleter(blossomReaderWriter blossomDeleter, HashKey key, bool flush)
        {
            lock (_getRoot())
            {
                var deleted = blossomDeleter.deleteBloomFileToFile(_blossomFilePosition, blossomID, key);

                if (deleted == Status.Successful)
                {
                    superAddKey(key);

                    if (flush == true)
                    {
                        blossomDeleter.flush();
                    }
                }

                return deleted;
            }
        }

        internal override bool containsKey(HashKey key, blossomReaderWriter blossomReaderWriter)
        {
            if (blossomReaderWriter.bloomFileBlockOffsetLookup != null)
            {
                int offset;
                return blossomReaderWriter.bloomFileBlockOffsetLookup.TryGetValue(key, out offset);
            }

            return base.containsKey(key, blossomReaderWriter);
        }

        internal void flushCreator(blossomReaderWriter blossomCreator)
        {
            if ((dirty & 0x01) == 0x01)
            {
                lock (_getRoot())
                {
                    if ((dirty & 0x01) == 0x01)
                    {
                        dirty &= 0xFE;
                        blossomCreator.flushBufferToFile(_blossomFilePosition);
                    }
                }
            }

            // by putting flush here we ensure that any outstanding writes are recognized
            blossomCreator.flush();
        }

        private bloomFileBranch _getRoot()
        {
            bloomFileBranch root = this;

            while (root.parent != null)
            {
                root = root.parent;
            }

            return root;
        }
    }
}
