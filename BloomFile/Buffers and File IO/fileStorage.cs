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
using System.IO;

namespace Phrenologix
{
    internal class fileStorage : IDisposable
    {
        private char[] _header = new char[] { 'B', 'L', 'O', 'O', 'M', 'F', 'I', 'L', 'E', '1' };
        private FileStream _fileStream;
        private BinaryWriter _binaryWriter;
        private BinaryReader _binaryReader;
        private long _lastFileBlockPosition;
        private int _totalBufferSize;
        private long _growFileSize;
        private int _fileID;

        internal int fileID
        {
            get
            {
                return _fileID;
            }
        }

        internal long firstFileBlockPosition
        {
            get
            {
                return (long)_header.Length;
            }
        }

        internal long lastFileBlockPosition
        {
            get
            {
                return _lastFileBlockPosition;
            }
        }

        internal fileStorage(int fileID, string filePath, int totalBufferSize, long initialFileSize, long growFileSize)
        {
            _fileID = fileID;

            int bufferSize = 8;

            _totalBufferSize = totalBufferSize;

            if (_totalBufferSize != 0)
            {
                bufferSize = _totalBufferSize;
            }

            _growFileSize = growFileSize;
            _fileStream = new FileStream(filePath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None, bufferSize, FileOptions.None); // writethrough | no buffering... 0x80000000 | 0x20000000            
            _binaryWriter = new BinaryWriter(_fileStream);
            _binaryReader = new BinaryReader(_fileStream);

            if (_fileStream.Length < initialFileSize)
            {
                _fileStream.SetLength(initialFileSize);
            }

            _binaryWriter.Write(_header);
            _lastFileBlockPosition = _fileStream.Position;
        }

        internal byte[] read(int fileBlockSize)
        {
            // read bytes data
            return _binaryReader.ReadBytes(fileBlockSize);
        }

        internal long read(int fileBlockSize, byte[] readBuffer)
        {
            var fileBlockPosition = _fileStream.Position;

            // read bytes data
            _binaryReader.Read(readBuffer, 0, fileBlockSize);
            return fileBlockPosition;
        }

        internal void read(long fileBlockPosition, int bufferLocation, int fileBlockSize, byte[] readBuffer)
        {
            if (_fileStream.Position != fileBlockPosition)
            {
                _fileStream.Seek(fileBlockPosition, SeekOrigin.Begin);
            }

            // read bytes data
            _binaryReader.Read(readBuffer, bufferLocation, fileBlockSize);
        }

        internal void write(long fileBlockPosition, int intToWrite)
        {
            if (_fileStream.Position != fileBlockPosition)
            {
                _fileStream.Seek(fileBlockPosition, SeekOrigin.Begin);
            }

            // write int data
            _binaryWriter.Write(intToWrite);
        }

        internal void write(long fileBlockPosition, byte[] bytesToWrite)
        {
            if (_fileStream.Position != fileBlockPosition)
            {
                _fileStream.Seek(fileBlockPosition, SeekOrigin.Begin);
            }

            // write bytes data
            _binaryWriter.Write(bytesToWrite);
        }

        internal void write(long fileBlockPosition, byte[] bytesToWrite, int sizeOfFirstSection, int startOfSecondSection, int sizeOfSecondSection)
        {
            if (_fileStream.Position != fileBlockPosition)
            {
                _fileStream.Seek(fileBlockPosition, SeekOrigin.Begin);
            }

            // write first section bytes data
            _binaryWriter.Write(bytesToWrite, 0, sizeOfFirstSection);

            // write second section bytes data
            _binaryWriter.Write(bytesToWrite, startOfSecondSection, sizeOfSecondSection);
        }

        internal long advanceNextFileBlockPosition(long offset)
        {
            _lastFileBlockPosition += offset;

            if (_lastFileBlockPosition + _totalBufferSize >= _fileStream.Length)
            {
                _fileStream.SetLength(_growFileSize + _fileStream.Length);
            }

            return _lastFileBlockPosition;
        }

        internal void flush()
        {
            _binaryWriter.Flush();
        }

        public void Dispose()
        {
            _binaryWriter.Flush();
            _binaryReader.Dispose();
            _binaryWriter.Dispose();
            _fileStream.Dispose();
            _binaryReader = null;
            _binaryWriter = null;
            _fileStream = null;
        }
    }
}
