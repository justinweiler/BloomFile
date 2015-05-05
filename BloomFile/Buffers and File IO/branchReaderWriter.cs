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


namespace Phrenologix
{
    internal class branchReaderWriter
    {
        private fileStorage     _fileStorageBranch;
        private byte[]          _theBuffer = new byte[ioConstants.branchOverhead];
        internal static bool    doComputeChecksum;

        internal branchReaderWriter(fileStorage fileStorageBranch)
        {
            _fileStorageBranch = fileStorageBranch;
        }

        unsafe internal byte[] readBranchFromFile(out long branchFilePosition, out byte level, out DateTime timestamp, out long blossomFilePosition, out int blossomID)
        {
            level               = 0;
            timestamp           = DateTime.MinValue;
            blossomFilePosition = 0;
            blossomID           = 0;

            byte[] dataBytes = null;

            fixed (byte* branchHeaderBytes = _theBuffer)
            {
                branchFilePosition = _fileStorageBranch.read(ioConstants.branchOverhead, _theBuffer); // read BRCH + level + timestamp + checksum + blossomFilePosition + blossomID + data size

                uint startOfBranchMarker = *((uint*)(branchHeaderBytes + 0));

                // branch corrupt or EOF
                if (startOfBranchMarker != ioConstants.startOfBranchMarker)
                {
                    bool corrupt = false;

                    // check to see if we are at the end of the file
                    for (int i = 0; i < ioConstants.branchOverhead; i++)
                    {
                        if (*(branchHeaderBytes + i) != 0)
                        {
                            corrupt = true;
                            break;
                        }
                    }

                    if (corrupt == true)
                    {
                        throw new InvalidDataException("branch corrupt");
                    }
                    else
                    {
                        return null; // signal EOF
                    }
                }

                level                    = *(branchHeaderBytes + 4);                                // read level
                timestamp                = DateTime.FromBinary(*((long*)(branchHeaderBytes + 5)));  // read timestamp
                var checksum             = *((uint*)(branchHeaderBytes + 13));                      // read checksum
                blossomFilePosition      = *((long*)(branchHeaderBytes + 17));                      // read blossomFilePosition
                blossomID                = *((int*)(branchHeaderBytes + 25));                       // read blossomID
                var dataBytesLength      = *((int*)(branchHeaderBytes + 29));                       // read data length

                if (dataBytesLength <= 0)
                {
                    throw new InvalidDataException("dataBytesLength underflow");
                }

                // read data from file on disk
                dataBytes = _fileStorageBranch.read(dataBytesLength);

                // compute checksum
                if (checksum > 0 && doComputeChecksum == true)
                {
                    var computedChecksum = checkSum.compute(dataBytes);

                    if (computedChecksum != checksum)
                    {
                        throw new InvalidDataException("failed checksum");
                    }
                }
            }

            return dataBytes;
        }

        internal long advanceNextBranchFilePosition(int lastBranchSize)
        {
            return _fileStorageBranch.advanceNextFileBlockPosition(lastBranchSize);
        }

        unsafe internal void writeBranchToFile(long branchFilePosition, byte[] dataBytes, byte level, DateTime timestamp, long blossomFilePosition, int blossomID)
        {
            fixed (byte* theBufferBytes = _theBuffer)
            {
                uint checksum = 0;

                // compute checksum
                if (doComputeChecksum == true)
                {
                    checksum = checkSum.compute(dataBytes);
                }

                // write branch header
                *((uint*)(theBufferBytes + 0))  = ioConstants.startOfBranchMarker;      // start of branch marker BRCH
                *(theBufferBytes + 4)           = level;                                // write level
                *((long*)(theBufferBytes + 5))  = timestamp.ToBinary();                 // write timestamp
                *((uint*)(theBufferBytes + 13)) = checksum;                             // write checksum
                *((long*)(theBufferBytes + 17)) = blossomFilePosition;                  // write blossomFilePosition
                *((int*)(theBufferBytes + 25))  = blossomID;                            // write blossomID
                *((int*)(theBufferBytes + 29))  = dataBytes.Length;                     // write data length

                // write data to file
                _fileStorageBranch.write(branchFilePosition, _theBuffer);
                _fileStorageBranch.write(branchFilePosition + ioConstants.branchOverhead, dataBytes);
            }
        }
    }
}
