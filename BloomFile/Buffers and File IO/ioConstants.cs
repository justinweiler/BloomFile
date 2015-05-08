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

namespace Phrenologix
{
    internal class ioConstants
    {
        internal static byte[] fenceKeyBytes = HashKey.Max.ToBytes();
        internal static HashKey fenceKey = HashKey.Max;
        internal const int bloomFileOverhead = 49;
        internal const int branchOverhead = 33;
        internal const int blossomOverhead = 16;
        internal const uint startOfBloomFileMarker = (uint)0x464D4C42; // BLMF
        internal const uint startOfBlossomMarker = (uint)0x4D534C42; // BLSM
        internal const uint startOfBranchMarker = (uint)0x48435242; // BRCH

        internal static int computeTotalBufferSize(int keyCapacity, int averageDataItemSize, double averageDataItemSizeSlack, out int bloomFileBlockStartPosition)
        {
            var bloomFileBlockSize = (int)((double)((averageDataItemSize * averageDataItemSizeSlack) + ioConstants.bloomFileOverhead) * (double)keyCapacity);
            var keyBlockSize = computeKeyBlockSize(keyCapacity, true);

            bloomFileBlockStartPosition = keyBlockSize;

            // key block + bloomFile block + reverse traversal
            var totalBufferSize = keyBlockSize + bloomFileBlockSize + 4;

            // round to memory page of 4096 bytes
            if (totalBufferSize % 4096 > 0)
            {
                int alu = (totalBufferSize / 4096) + 1;
                totalBufferSize = alu * 4096;
            }

            return totalBufferSize;
        }

        internal static int computeKeyBlockSize(int keysUsed, bool includeFence)
        {
            // BLSM + blossom id + actual file block size + keys used + (capacity * key/location) + fence
            return blossomOverhead + (keysUsed * (24)) + (includeFence ? 20 : 0);
        }
    }
}
