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


namespace Phrenologix
{
    internal class bloomFileBranch
    {
        private List<bloomFileBranch>  _branchList;
        private bloomFileBranch        _parent;
        private byte                _level;   
        private byte[]              _hashBits;
        private long                _branchFilePosition;
        private int                 _bitCount;
        protected volatile byte     dirty;
        private static byte         _hashFunctionCount;
        internal static byte[]      branchFactors;

#if TRACKFP
        private int              _falsePositives = 0;
#if TRACKFPLOOKUP        
        private HashSet<HashKey> _itemLookup = null;
#endif
#endif

        internal byte level
        {
            get
            {
                return _level;
            }
        }

        internal bloomFileBranch parent
        {
            get
            {
                return _parent;
            }
        }

        internal int branchCount
        {
            get
            {
                return _branchList.Count;
            }
        }

        internal bloomFileBranch(long branchFilePosition, byte level, int keyCapacity, float errorRate, byte[] hashBits = null)
        {
#if USEPRECALCBRCH
            int m = 0;
            int k = 6;

            switch (level)
            {
                case 1: m = 774;        break;
                case 2: m = 3103;       break;
                case 3: m = 24843;      break;
                case 4: m = 397568;     break;
                case 5: m = 12722378;   break;
            }
#else
            int m = _bestM(keyCapacity, errorRate);
            int k = _bestK(keyCapacity, errorRate);
#endif

            if (hashBits != null)
            {
                _hashBits = hashBits;
            }
            else
            {
                _hashBits = new byte[(int)Math.Ceiling((double)m / 8)];
            }

            _level                   = level;
            _hashFunctionCount       = (byte)k;
            _bitCount                = _hashBits.Length << 3;
            _branchFilePosition = branchFilePosition;

#if TRACKFP && TRACKFPLOOKUP
            if (_level == 1)
            {
                _itemLookup = new HashSet<HashKey>();
            }
#endif
        }

#if TRACKFP
        internal int countFalsePositives(int level)
        {
            if (level == _level)
            {
                return _falsePositives;
            }

            int total = 0;

            foreach (var branch in _branches)
            {
                total += branch.countFalsePositives(level);
            }

            return total;
        }

        internal void clearFalsePositives()
        {
            _falsePositives = 0;

            if (_branches != null)
            {
                foreach (var branch in _branches)
                {
                    branch.clearFalsePositives();
                }
            }
        }
#endif

        internal void flushBloomFileBranch(branchReaderWriter readWriteBranch, ref long lastBranchFilePosition, DateTime timestamp)
        {
            if ((dirty & 0x10) == 0x10)
            {
                dirty &= 0xEF;

                if (_branchFilePosition == 0)
                {                    
                    _branchFilePosition    = lastBranchFilePosition;
                    lastBranchFilePosition = readWriteBranch.advanceNextBranchFilePosition(ioConstants.branchOverhead + _hashBits.Length);
                }

                long blossomFilePosition = 0;
                int blossomID            = 0;

                if (this is bloomFileBlossom)
                {
                    blossomFilePosition = ((bloomFileBlossom)this).blossomFilePosition;
                    blossomID           = ((bloomFileBlossom)this).blossomID;
                }

                readWriteBranch.writeBranchToFile(_branchFilePosition, _hashBits, _level, timestamp, blossomFilePosition, blossomID);

                if (_branchList != null)
                {
                    var branchCount = _branchList.Count;

                    for (int i = 0; i < branchCount; i++)
                    {
                        _branchList[i].flushBloomFileBranch(readWriteBranch, ref lastBranchFilePosition, timestamp);
                    }
                }
            }
        }

        internal void addChildBranch(bloomFileBranch child)
        {
            if (_branchList == null)
            {
                _branchList = new List<bloomFileBranch>(branchFactors[level - 1]);
            }
            
            _branchList.Add(child);
            child._parent = this;
        }

        internal Status superContainsKey(HashKey key, blossomReaderWriter blossomReaderWriter, Func<bloomFileBlossom, Status> evaluator)
        {
            if (containsKey(key, blossomReaderWriter) == false)
            {
                return Status.KeyNotFound;
            }

            if (_branchList != null)
            {
                Status foundBranch = Status.KeyNotFound;

                for (int i = _branchList.Count - 1; i >= 0; i--)
                {
                    foundBranch = _branchList[i].superContainsKey(key, blossomReaderWriter, evaluator);

                    if (foundBranch != Status.KeyNotFound)
                    {
                        break;
                    }
                }

#if TRACKFP
                if (foundBranch == Status.KeyNotFound)
                {
                    _falsePositives++;
                }
#endif
                return foundBranch;
            }
            else
            {
                if (evaluator != null)
                {
                    Status evaluation = evaluator((bloomFileBlossom)this);

#if TRACKFP
                    if (evaluation == Status.KeyNotFound)
                    {
                        _falsePositives++;
                    }
#endif

                    return evaluation;
                }

#if TRACKFP && TRACKFPLOOKUP
                else
                {
                    if (_itemLookup != null && _itemLookup.Contains(key) == false)
                    {
                        _falsePositives++;
                        return Status.KeyNotFound;
                    }
                }
#endif

                return Status.Successful;
            }
        }

        internal void superAddKey(HashKey key)
        {
            _addKey(key);

            if (_parent != null)
            {
                _parent.superAddKey(key);
            }

#if TRACKFP && TRACKFPLOOKUP
            if (_itemLookup != null)
            {
                _itemLookup.Add(key);
            }
#endif
        }
        
        internal virtual bool containsKey(HashKey key, blossomReaderWriter blossomReaderWriter)
        {
            int primaryHash   = key.GetHashCode();
            int secondaryHash = key.GetHashCode(_level);
            
            for (int i = 0; i < _hashFunctionCount; i++)
            {
                int hash = _computeHash(primaryHash, secondaryHash, i);

                if (_testBit(hash) == false)
                {
                    return false;
                }
            }
            
            return true;
        }

        private void _setBit(int bit)
        {
            var div = bit >> 3;
            var rem = (div << 3) ^ bit;
            _hashBits[div] |= (byte)(0x01 << rem);
        }

        private bool _testBit(int bit)
        {
            var div = bit >> 3;
            var rem = (div << 3) ^ bit;
            return (_hashBits[div] & (byte)(0x01 << rem)) != 0;
        }

        private void _addKey(HashKey key)
        {
            // start flipping bits for each hash of item
            int primaryHash   = key.GetHashCode();
            int secondaryHash = key.GetHashCode(_level);

            for (int i = 0; i < _hashFunctionCount; i++)
            {
                int hash = _computeHash(primaryHash, secondaryHash, i);
                _setBit(hash);
            }

            dirty |= 0x10;
        }

        // Performs Dillinger and Manolios double hashing.
        // TODO: Instead of MOD can we shift and adjust tree to be power of 2?
        // TODO: Suspect there is a better way to do hashing and ensure against collisions looping with the same input feels like it will create anomalies
        private int _computeHash(int primaryHash, int secondaryHash, int i)
        {
            int resultingHash = (primaryHash + (i * secondaryHash)) % _bitCount;
            return Math.Abs(resultingHash);
        }

        private static int _bestK(int keyCapacity, float errorRate)
        {
            return (int)Math.Round(Math.Log(2.0) * _bestM(keyCapacity, errorRate) / keyCapacity);
        }

        private static int _bestM(int keyCapacity, float errorRate)
        { 
            return (int)Math.Ceiling(keyCapacity * Math.Log(errorRate, (1.0 / Math.Pow(2, Math.Log(2.0)))));
        }

        /*
        private static float _bestErrorRate(int keyCapacity)
        {
            float c = (float)(1.0 / keyCapacity);

            if (c != 0)
            {
                return c;
            }
            else
            {
                return (float)Math.Pow(0.6185, int.MaxValue / keyCapacity); // http://www.cs.princeton.edu/courses/archive/spring02/cs493/lec7.pdf
            }
        }
        */
    }
}
