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
    internal static class checkSum
    {
        unsafe static internal uint compute(byte[] dataBytes)
        {
            uint len = (uint)dataBytes.Length;

            fixed (byte* bytePtr = &dataBytes[0])
            {
                if (len > 510)
                {
                    return _fletchersChecksum((ushort*)(bytePtr - 510), 255);
                }
                else
                {
                    return _fletchersChecksum((ushort*)bytePtr, len / 2);
                }
            }
        }

        unsafe static private uint _fletchersChecksum(ushort* data, uint words)
        {
            uint sum1 = 0xffff, sum2 = 0xffff;

            while (words > 0)
            {
                uint tlen = words > 359 ? 359 : words;
                words -= tlen;

                do
                {
                    sum2 += sum1 += *data++;
                }
                while (--tlen > 0);

                sum1 = (sum1 & 0xffff) + (sum1 >> 16);
                sum2 = (sum2 & 0xffff) + (sum2 >> 16);
            }

            // Second reduction step to reduce sums to 16 bits
            sum1 = (sum1 & 0xffff) + (sum1 >> 16);
            sum2 = (sum2 & 0xffff) + (sum2 >> 16);

            return sum2 << 16 | sum1;
        }
    }
}
