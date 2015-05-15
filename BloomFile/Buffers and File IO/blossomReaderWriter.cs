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
using Phrenologix.Lib;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;

namespace Phrenologix
{
    internal class blossomReaderWriter
    {
        private int _keyCapacity;
        private int _keysUsed;
        private byte[] _theBuffer;
        private fileStorage _fileStorageBlossom;
        private int _bloomFileBlockStartPosition;
        private int _bloomFileBlockEndPosition;
        private int _blossomID;
        internal SortedList<HashKey, int> bloomFileBlockOffsetLookup;
        internal static bool doComputeChecksum;
        internal static TimeSpan timeSlipWindow = TimeSpan.FromSeconds(30);

        internal blossomReaderWriter(fileStorage fileStorageBlossom, int keyCapacity, int totalBufferSize, int bloomFileBlockStartPosition, bool createAsCurrent)
        {
            _keyCapacity = keyCapacity;
            _fileStorageBlossom = fileStorageBlossom;
            _theBuffer = new byte[totalBufferSize];  // last 4 bytes here are for reverse traversal
            _bloomFileBlockStartPosition = bloomFileBlockStartPosition;
            _bloomFileBlockEndPosition = _bloomFileBlockStartPosition;

            if (createAsCurrent == true)
            {
                bloomFileBlockOffsetLookup = new SortedList<HashKey, int>(_keyCapacity);
            }
        }

        unsafe internal void createBloomFileToBuffer(HashKey key, byte[] dataBytes, byte recordType, DateTime timestamp, int versionNo, float slackFactor)
        {
            if (bloomFileBlockOffsetLookup == null)
            {
                throw new FieldCorruptException("bloomFileBlockOffsetLookup");
            }

            fixed (byte* theBufferBytes = _theBuffer)
            {
                var bloomFileHeaderBytes = theBufferBytes + _bloomFileBlockEndPosition;

                uint checksum = 0;

                // compute checksum
                if (doComputeChecksum == true)
                {
                    checksum = checkSum.compute(dataBytes);
                }

                int dataSlotSize = (int)(dataBytes.Length * slackFactor);

                // write bloomFile header
                *((uint*)(bloomFileHeaderBytes + 0)) = ioConstants.startOfBloomFileMarker;  // start of bloomFile marker BLMF
                *(bloomFileHeaderBytes + 4) = recordType;                                   // write recordType
                *((long*)(bloomFileHeaderBytes + 5)) = timestamp.ToBinary();                // write timestamp
                *((int*)(bloomFileHeaderBytes + 13)) = versionNo;                           // write versionNo
                *((uint*)(bloomFileHeaderBytes + 17)) = key.uint1;                          // write key.uint1
                *((uint*)(bloomFileHeaderBytes + 21)) = key.uint2;                          // write key.uint2
                *((uint*)(bloomFileHeaderBytes + 25)) = key.uint3;                          // write key.uint3
                *((uint*)(bloomFileHeaderBytes + 29)) = key.uint4;                          // write key.uint4
                *((uint*)(bloomFileHeaderBytes + 33)) = key.uint5;                          // write key.uint5
                *((uint*)(bloomFileHeaderBytes + 37)) = checksum;                           // write checksum
                *((int*)(bloomFileHeaderBytes + 41)) = dataSlotSize;                        // write data slot size
                *((int*)(bloomFileHeaderBytes + 45)) = dataBytes.Length;                    // write data length

                // write data and slack padding
                var bloomFileDataBytes = bloomFileHeaderBytes + ioConstants.bloomFileOverhead;
                var dataBytesCnt = dataBytes.Length;

                int i = 0;

                // write data
                for (; i < dataBytesCnt; i++)
                {
                    *(bloomFileDataBytes + i) = dataBytes[i];
                }

                // write slack padding
                for (; i < dataSlotSize; i++)
                {
                    *(bloomFileDataBytes + i) = 0;
                }

                // check if previously existed
                int offset;
                bool previouslyExisted = bloomFileBlockOffsetLookup.TryGetValue(key, out offset);

                // update offset table                
                bloomFileBlockOffsetLookup[key] = _bloomFileBlockEndPosition - _bloomFileBlockStartPosition;

                // update end block position
                _bloomFileBlockEndPosition = _bloomFileBlockEndPosition + ioConstants.bloomFileOverhead + i;

                // increment keys used if new key
                if (previouslyExisted == false)
                {
                    _keysUsed++;
                }
            }
        }

        unsafe internal Status readBloomFileFromFile(long blossomFilePosition, int blossomID, HashKey key, out byte[] dataBytes, out byte recordType, out DateTime timestamp, out int versionNo)
        {
            // setup defaults
            recordType = 0;
            timestamp = DateTime.MinValue;
            versionNo = 0;
            dataBytes = null;

            fixed (byte* blossomHeaderBytes = _theBuffer)
            {
                int sizeOfKeyBlock, sizeOfBloomFileBlock;
                _readBlossomHeaderAndKeys(blossomFilePosition, blossomID, blossomHeaderBytes, out sizeOfKeyBlock, out sizeOfBloomFileBlock);

                // binary search for key, courtesy wikipedia
                // continually narrow search until just one element remains
                int iMin = 0;
                int iMax = _keysUsed;

                uint* keyDataBytes = (uint*)(blossomHeaderBytes + ioConstants.blossomOverhead);

                while (iMin < iMax)
                {
                    int iMid = (iMin + iMax) / 2;

                    int offset;
                    var testKey = _extractHashKey(keyDataBytes, iMid, out offset);

                    // reduce the search or terminate if found
                    if (testKey == key)
                    {
                        if (offset == -1)
                        {
                            return Status.KeyFoundButMarkedDeleted;
                        }

                        var bloomFilePos = offset + sizeOfKeyBlock + blossomFilePosition;                                      // compute position       
                        _fileStorageBlossom.read(bloomFilePos, 0, ioConstants.bloomFileOverhead, _theBuffer);                  // read bloomFile header from file block             
                        dataBytes = _readBloomFile(_fileStorageBlossom, 0, key, out recordType, out timestamp, out versionNo); // read bloomFile
                        return Status.Successful;
                    }
                    else if (testKey < key)
                    {
                        iMin = iMid + 1;
                    }
                    else
                    {
                        iMax = iMid;
                    }
                }
            }

            return Status.KeyNotFound;
        }

        internal Status readBloomFileFromBuffer(HashKey key, out byte[] dataBytes, out byte recordType, out DateTime timestamp, out int versionNo)
        {
            if (bloomFileBlockOffsetLookup == null)
            {
                throw new FieldCorruptException("bloomFileBlockOffsetLookup");
            }

            // setup defaults
            recordType = 0;
            timestamp = DateTime.MinValue;
            versionNo = 0;
            dataBytes = null;

            int offset;

            if (bloomFileBlockOffsetLookup.TryGetValue(key, out offset) == false)
            {
                return Status.KeyNotFound;
            }

            if (offset == -1)
            {
                return Status.KeyFoundButMarkedDeleted;
            }

            // read bloomFile
            dataBytes = _readBloomFile(null, offset + _bloomFileBlockStartPosition, key, out recordType, out timestamp, out versionNo);
            return Status.Successful;
        }

        unsafe internal Status updateBloomFileToFile(long blossomFilePosition, int blossomID, HashKey key, byte[] dataBytes, byte recordType, ref DateTime timestamp, ref int versionNo)
        {
            fixed (byte* blossomHeaderBytes = _theBuffer)
            {
                int sizeOfKeyBlock, sizeOfBloomFileBlock;
                _readBlossomHeaderAndKeys(blossomFilePosition, blossomID, blossomHeaderBytes, out sizeOfKeyBlock, out sizeOfBloomFileBlock);

                // binary search for key, courtesy wikipedia
                // continually narrow search until just one element remains
                int iMin = 0;
                int iMax = _keysUsed;

                uint* keyDataBytes = (uint*)(blossomHeaderBytes + ioConstants.blossomOverhead);

                while (iMin < iMax)
                {
                    int iMid = (iMin + iMax) / 2;

                    int offset;
                    var testKey = _extractHashKey(keyDataBytes, iMid, out offset);

                    // reduce the search or terminate if found
                    if (testKey == key)
                    {
                        if (offset == -1)
                        {
                            return Status.KeyFoundButMarkedDeleted;
                        }

                        var bloomFileFilePosition = offset + sizeOfKeyBlock + blossomFilePosition;                                                     // compute position       
                        _fileStorageBlossom.read(bloomFileFilePosition, 0, ioConstants.bloomFileOverhead, _theBuffer);                                    // read bloomFile header from file block                                    
                        return _updateBloomFile(_fileStorageBlossom, bloomFileFilePosition, 0, key, dataBytes, recordType, ref timestamp, ref versionNo); // update bloomFile
                    }
                    else if (testKey < key)
                    {
                        iMin = iMid + 1;
                    }
                    else
                    {
                        iMax = iMid;
                    }
                }
            }

            return Status.KeyNotFound;
        }

        internal Status updateBloomFileToBuffer(HashKey key, byte[] dataBytes, byte recordType, ref DateTime timestamp, ref int versionNo)
        {
            if (bloomFileBlockOffsetLookup == null)
            {
                throw new FieldCorruptException("bloomFileBlockOffsetLookup");
            }

            int offset;

            if (bloomFileBlockOffsetLookup.TryGetValue(key, out offset) == false)
            {
                return Status.KeyNotFound;
            }

            if (offset == -1)
            {
                return Status.KeyFoundButMarkedDeleted;
            }

            // update bloomFile
            return _updateBloomFile(null, 0, (int)offset + _bloomFileBlockStartPosition, key, dataBytes, recordType, ref timestamp, ref versionNo);
        }

        unsafe internal Status deleteBloomFileToFile(long blossomFilePosition, int blossomID, HashKey key)
        {
            fixed (byte* blossomHeaderBytes = _theBuffer)
            {
                int sizeOfKeyBlock, sizeOfBloomFileBlock;
                _readBlossomHeaderAndKeys(blossomFilePosition, blossomID, blossomHeaderBytes, out sizeOfKeyBlock, out sizeOfBloomFileBlock);

                // binary search for key, courtesy wikipedia
                // continually narrow search until just one element remains
                int iMin = 0;
                int iMax = _keysUsed;

                uint* keyDataBytes = (uint*)(blossomHeaderBytes + ioConstants.blossomOverhead);

                while (iMin < iMax)
                {
                    int iMid = (iMin + iMax) / 2;

                    int offset;
                    var testKey = _extractHashKey(keyDataBytes, iMid, out offset);

                    // reduce the search or terminate if found
                    if (testKey == key)
                    {
                        if (offset == -1)
                        {
                            return Status.KeyFoundButMarkedDeleted;
                        }

                        var keyOffsetFileBlockPosition = blossomFilePosition + ioConstants.blossomOverhead + ((6 * iMid) + 5) * 4;
                        _fileStorageBlossom.write(keyOffsetFileBlockPosition, -1); // cancel offset       
                        return Status.Successful;
                    }
                    else if (testKey < key)
                    {
                        iMin = iMid + 1;
                    }
                    else
                    {
                        iMax = iMid;
                    }
                }
            }

            return Status.KeyNotFound;
        }

        internal Status deleteBloomFileToBuffer(HashKey key)
        {
            if (bloomFileBlockOffsetLookup == null)
            {
                throw new FieldCorruptException("bloomFileBlockOffsetLookup");
            }

            int offset;

            if (bloomFileBlockOffsetLookup.TryGetValue(key, out offset) == false)
            {
                return Status.KeyNotFound;
            }

            if (offset == -1)
            {
                return Status.KeyFoundButMarkedDeleted;
            }

            bloomFileBlockOffsetLookup[key] = -1;
            return Status.Successful;
        }

        unsafe internal void reset(int blossomID)
        {
            if (bloomFileBlockOffsetLookup == null)
            {
                throw new FieldCorruptException("bloomFileBlockOffsetLookup");
            }

            // reset fields to starting values
            _blossomID = blossomID;
            _bloomFileBlockEndPosition = _bloomFileBlockStartPosition;
            _keysUsed = 0;

            bloomFileBlockOffsetLookup.Clear();
            bloomFileBlockOffsetLookup.Add(ioConstants.fenceKey, 0);

            fixed (byte* blossomHeaderBytes = _theBuffer)
            {
                // initialize or reset buffer
                *((uint*)(blossomHeaderBytes + 0)) = ioConstants.startOfBlossomMarker;  // start of blossom marker BLSM
                *((int*)(blossomHeaderBytes + 4)) = _blossomID;                         // blossom id
                *((int*)(blossomHeaderBytes + 8)) = computeBlossomFileSize();           // actual blossom file size
                *((int*)(blossomHeaderBytes + 12)) = _keysUsed;                         // keys used

                var keyDataBytes = blossomHeaderBytes + ioConstants.blossomOverhead;

                // write fence key bytes
                for (int i = 0; i < 20; i++)
                {
                    *(keyDataBytes + i) = ioConstants.fenceKeyBytes[i];
                }
            }
        }

        unsafe internal void restoreFromFile(long blossomFilePosition, int blossomID)
        {
            if (bloomFileBlockOffsetLookup == null)
            {
                throw new FieldCorruptException("bloomFileBlockOffsetLookup");
            }

            fixed (byte* blossomHeaderBytes = _theBuffer)
            {
                int sizeOfKeyBlock, sizeOfBloomFileBlock;
                _readBlossomHeaderAndKeys(blossomFilePosition, blossomID, blossomHeaderBytes, out sizeOfKeyBlock, out sizeOfBloomFileBlock);

                uint* keyDataBytes = (uint*)(blossomHeaderBytes + ioConstants.blossomOverhead);

                bloomFileBlockOffsetLookup.Clear();
                bloomFileBlockOffsetLookup.Add(ioConstants.fenceKey, 0);

                for (int i = 0; i < _keysUsed; i++)
                {
                    int offset;
                    var key = _extractHashKey(keyDataBytes, i, out offset);
                    bloomFileBlockOffsetLookup[key] = offset;
                }

                _fileStorageBlossom.read(blossomFilePosition + sizeOfKeyBlock, _bloomFileBlockStartPosition, sizeOfBloomFileBlock, _theBuffer); // read all cherries
                _fileStorageBlossom.advanceNextFileBlockPosition(blossomFilePosition + sizeOfKeyBlock + sizeOfBloomFileBlock - _fileStorageBlossom.firstFileBlockPosition);
            }
        }

        internal bool canCreate(byte[] dataBytes)
        {
            if (bloomFileBlockOffsetLookup == null)
            {
                return false;
            }

            // too many keys = cant create
            if (_keysUsed == _keyCapacity)
            {
                return false;
            }

            // too little size left = cant create (saving space for reverse traversal offset)
            if (_bloomFileBlockEndPosition + ioConstants.bloomFileOverhead + dataBytes.Length > _theBuffer.Length - 4)
            {
                return false;
            }

            return true;
        }

        internal void flush()
        {
            _fileStorageBlossom.flush();
        }

        unsafe internal void flushBufferToFile(long blossomFilePosition)
        {
            if (bloomFileBlockOffsetLookup == null)
            {
                throw new FieldCorruptException("bloomFileBlockOffsetLookup");
            }

            fixed (byte* blossomHeaderBytes = _theBuffer)
            {
                // got to end of block and write reverse traversal offset
                _bloomFileBlockEndPosition += 4;
                *((int*)(blossomHeaderBytes + (_bloomFileBlockEndPosition - 4))) = computeBlossomFileSize();

                // write actual blossom file size and keys used, write after BLSM marker and ID
                *((int*)(blossomHeaderBytes + 8)) = computeBlossomFileSize();
                *((int*)(blossomHeaderBytes + 12)) = _keysUsed;

                // write sorted list of keys, include fence key
                uint* keyDataBytes = (uint*)(blossomHeaderBytes + ioConstants.blossomOverhead);

                for (int i = 0; i <= _keysUsed; i++)
                {
                    var key = bloomFileBlockOffsetLookup.Keys[i];
                    var offset = bloomFileBlockOffsetLookup.Values[i];

                    // write key
                    *(keyDataBytes + (6 * i) + 0) = key.uint1;  // write key.uint1
                    *(keyDataBytes + (6 * i) + 1) = key.uint2;  // write key.uint2
                    *(keyDataBytes + (6 * i) + 2) = key.uint3;  // write key.uint3
                    *(keyDataBytes + (6 * i) + 3) = key.uint4;  // write key.uint4
                    *(keyDataBytes + (6 * i) + 4) = key.uint5;  // write key.uint5

                    if (i < _keysUsed) // write offset of everything but fence key
                    {
                        *((int*)(keyDataBytes + (6 * i) + 5)) = offset;   // write bloomFile offset from the start of the bloomFile block
                    }
                }

                // write blossom to disk
                _fileStorageBlossom.write(blossomFilePosition, _theBuffer, ioConstants.computeKeyBlockSize(_keysUsed, true), _bloomFileBlockStartPosition, _bloomFileBlockEndPosition - _bloomFileBlockStartPosition);
            }
        }

        internal int computeBlossomFileSize()
        {
            return ioConstants.computeKeyBlockSize(_keysUsed, true) + (_bloomFileBlockEndPosition - _bloomFileBlockStartPosition);
        }

        unsafe private static HashKey _extractHashKey(uint* keyDataBytes, int i, out int offset)
        {
            uint uint1 = *(keyDataBytes + (6 * i) + 0);        // read key uint1       
            uint uint2 = *(keyDataBytes + (6 * i) + 1);        // read key uint2       
            uint uint3 = *(keyDataBytes + (6 * i) + 2);        // read key uint3       
            uint uint4 = *(keyDataBytes + (6 * i) + 3);        // read key uint4       
            uint uint5 = *(keyDataBytes + (6 * i) + 4);        // read key uint5       
            offset = *((int*)(keyDataBytes + (6 * i) + 5));    // read offset       

            return new HashKey(uint1, uint2, uint3, uint4, uint5);
        }

        unsafe private void _readBlossomHeaderAndKeys(long blossomFilePosition, int blossomID, byte* blossomHeaderBytes, out int sizeOfKeyBlock, out int sizeOfBloomFileBlock)
        {
            _fileStorageBlossom.read(blossomFilePosition, 0, ioConstants.blossomOverhead, _theBuffer); // read header BLSM + [ID] + [size] + keys used

            uint startOfBlossomMarker = *((uint*)(blossomHeaderBytes + 0));

            if (startOfBlossomMarker != ioConstants.startOfBlossomMarker)
            {
                throw new InvalidDataException("blossom corrupt");
            }

            int foundBlossomID = *((int*)(blossomHeaderBytes + 4));

            if (foundBlossomID != blossomID)
            {
                throw new InvalidDataException("blossomID mismatch");
            }

            int totalBlossomFileSize = *((int*)(blossomHeaderBytes + 8));

            _keysUsed = *((int*)(blossomHeaderBytes + 12));

            if (_keysUsed > _keyCapacity)
            {
                throw new InvalidDataException("_keysUsed overflow");
            }

            sizeOfKeyBlock = ioConstants.computeKeyBlockSize(_keysUsed, true);
            sizeOfBloomFileBlock = totalBlossomFileSize - sizeOfKeyBlock;
            _bloomFileBlockEndPosition = _bloomFileBlockStartPosition + sizeOfBloomFileBlock;

            _fileStorageBlossom.read(blossomFilePosition + ioConstants.blossomOverhead, ioConstants.blossomOverhead, sizeOfKeyBlock - ioConstants.blossomOverhead, _theBuffer); // read all keys
        }

        unsafe private byte[] _readBloomFile(fileStorage fileStorage, long bloomFileBufferPosition, HashKey key, out byte recordType, out DateTime timestamp, out int versionNo)
        {
            fixed (byte* theBufferBytes = _theBuffer)
            {
                byte* bloomFileHeaderBytes = theBufferBytes;

                // from the write buffer - if we already have the bloomFile contained within theBuffer
                if (bloomFileBufferPosition != 0)
                {
                    bloomFileHeaderBytes = bloomFileHeaderBytes + bloomFileBufferPosition;
                }

                var startOfBloomFileMarker = *((uint*)bloomFileHeaderBytes); // read start of bloomFile marker BLMF

                // validate start of bloomFile marker
                if (startOfBloomFileMarker != ioConstants.startOfBloomFileMarker)
                {
                    throw new InvalidDataException("bloomFile corrupt");
                }

                // read rest of bloomFile header
                recordType = *(bloomFileHeaderBytes + 4);                                // read recordType
                timestamp = DateTime.FromBinary(*((long*)(bloomFileHeaderBytes + 5)));   // read timestamp
                versionNo = *((int*)(bloomFileHeaderBytes + 13));                        // read versionNo
                var uint1 = *((uint*)(bloomFileHeaderBytes + 17));                       // read key uint1       
                var uint2 = *((uint*)(bloomFileHeaderBytes + 21));                       // read key uint2       
                var uint3 = *((uint*)(bloomFileHeaderBytes + 25));                       // read key uint3       
                var uint4 = *((uint*)(bloomFileHeaderBytes + 29));                       // read key uint4       
                var uint5 = *((uint*)(bloomFileHeaderBytes + 33));                       // read key uint5       

                // vaidate recovered key
                if (uint1 != key.uint1 || uint2 != key.uint2 || uint3 != key.uint3 || uint4 != key.uint4 || uint5 != key.uint5)
                {
                    throw new InvalidDataException("key mismatch");
                }

                // get checksum, slot size, and length
                var checksum = *((uint*)(bloomFileHeaderBytes + 37));           // read checksum
                var dataSlotSize = *((int*)(bloomFileHeaderBytes + 41));        // read data slot size
                var dataBytesLength = *((int*)(bloomFileHeaderBytes + 45));     // read data length

                // validate data slot size
                if (dataSlotSize <= 0)
                {
                    throw new InvalidDataException("dataSlotSize underflow");
                }

                // validate data bytes length
                if (dataBytesLength <= 0)
                {
                    throw new InvalidDataException("dataBytesLength underflow");
                }

                // validate data slot size versus data bytes length
                if (dataBytesLength > dataSlotSize)
                {
                    throw new InvalidDataException("dataBytesLength overflow");
                }

                byte[] dataBytes = null;

                // read data from file on disk or in buffer
                if (fileStorage != null)
                {
                    dataBytes = fileStorage.read(dataBytesLength); // read data

                    if (dataBytes.Length < dataBytesLength)
                    {
                        throw new InvalidDataException("dataBytes.Length underflow");
                    }
                }
                else
                {
                    var bloomFileDataBytes = bloomFileHeaderBytes + ioConstants.bloomFileOverhead;

                    dataBytes = new byte[dataBytesLength];

                    for (int i = 0; i < dataBytesLength; i++)
                    {
                        dataBytes[i] = *(bloomFileDataBytes + i);
                    }
                }

                // validate checksum
                if (checksum > 0 && doComputeChecksum == true)
                {
                    var computedChecksum = checkSum.compute(dataBytes);

                    if (computedChecksum != checksum)
                    {
                        throw new InvalidDataException("failed checksum");
                    }
                }

                return dataBytes;
            }
        }

        unsafe private Status _updateBloomFile(fileStorage fileStorage, long bloomFileFilePosition, int bloomFileBufferPosition, HashKey key, byte[] dataBytes, byte recordType, ref DateTime timestamp, ref int versionNo)
        {
            fixed (byte* theBufferBytes = _theBuffer)
            {
                byte* bloomFileHeaderBytes = theBufferBytes;

                // from the write buffer - if we already have the bloomFile contained within theBuffer
                if (bloomFileBufferPosition != 0)
                {
                    bloomFileHeaderBytes = bloomFileHeaderBytes + bloomFileBufferPosition;
                }

                var startOfBloomFileMarker = *((uint*)bloomFileHeaderBytes);   // read start of bloomFile marker BLMF

                // validate start of bloomFile marker
                if (startOfBloomFileMarker != ioConstants.startOfBloomFileMarker)
                {
                    throw new InvalidDataException("bloomFile corrupt");
                }

                // read rest of bloomFile header
                var oldTimestamp = DateTime.FromBinary(*((long*)(bloomFileHeaderBytes + 5)));   // read timestamp
                var oldVersionNo = *((int*)(bloomFileHeaderBytes + 13));                        // read versionNo
                var uint1 = *((uint*)(bloomFileHeaderBytes + 17));                              // read key uint1       
                var uint2 = *((uint*)(bloomFileHeaderBytes + 21));                              // read key uint2       
                var uint3 = *((uint*)(bloomFileHeaderBytes + 25));                              // read key uint3       
                var uint4 = *((uint*)(bloomFileHeaderBytes + 29));                              // read key uint4       
                var uint5 = *((uint*)(bloomFileHeaderBytes + 33));                              // read key uint5       

                // vaidate recovered key
                if (uint1 != key.uint1 || uint2 != key.uint2 || uint3 != key.uint3 || uint4 != key.uint4 || uint5 != key.uint5)
                {
                    throw new InvalidDataException("key mismatch");
                }

                // bump version no if requested by using zero or oldVersionNo
                if (versionNo == 0 || versionNo == oldVersionNo)
                {
                    versionNo = oldVersionNo + 1;
                }
                else if (versionNo < oldVersionNo)
                {
                    return Status.KeyVersionConflict;
                }

                // if using timestamps, validate and correct if necessary
                if (timestamp != DateTime.MinValue && timestamp != DateTime.MaxValue)
                {
                    // validate timestamp
                    if (oldTimestamp != DateTime.MaxValue && timestamp + timeSlipWindow < oldTimestamp)
                    {
                        return Status.KeyTimestampConflict;
                    }

                    // correct timestamp if necessary
                    if (timestamp <= oldTimestamp)
                    {
                        timestamp = DateTime.Now;
                    }
                }

                // get data slot size
                var dataSlotSize = *((int*)(bloomFileHeaderBytes + 41));

                // validate data slot size
                if (dataSlotSize <= 0)
                {
                    throw new InvalidDataException("dataSlotSize underflow");
                }

                // only allow inplace updates if available space
                if (dataSlotSize < dataBytes.Length)
                {
                    return Status.Unsuccessful;
                }

                uint checksum = 0;

                // compute checksum
                if (doComputeChecksum == true)
                {
                    checksum = checkSum.compute(dataBytes);
                }

                *(bloomFileHeaderBytes + 4) = recordType;                       // write recordType
                *((long*)(bloomFileHeaderBytes + 5)) = timestamp.ToBinary();    // write timestamp
                *((int*)(bloomFileHeaderBytes + 13)) = versionNo;               // write versionNo
                *((uint*)(bloomFileHeaderBytes + 37)) = checksum;               // write checksum
                *((int*)(bloomFileHeaderBytes + 45)) = dataBytes.Length;        // write new length

                var bloomFileDataBytes = bloomFileHeaderBytes + ioConstants.bloomFileOverhead;

                int i = 0;

                // write data
                for (; i < dataBytes.Length; i++)
                {
                    *(bloomFileDataBytes + i) = dataBytes[i];
                }

                // write slack padding
                for (; i < dataSlotSize; i++)
                {
                    *(bloomFileDataBytes + i) = 0;
                }

                // write data to file if applicable
                if (fileStorage != null)
                {
                    fileStorage.write(bloomFileFilePosition, _theBuffer, ioConstants.bloomFileOverhead, ioConstants.bloomFileOverhead, dataSlotSize); // write data
                }

                return Status.Successful;
            }
        }
    }
}
