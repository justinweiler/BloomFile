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
using System.Security.Cryptography;
using System.Text;


namespace Phrenologix
{
	[Serializable]
	public struct HashKey : IComparable, IComparable<HashKey>
	{
		public static uint Size
		{
			get
			{
				return 20;
			}
		}

		public static HashKey Max
		{
			get
			{
				byte[] maxBytes = new byte[]{ 255,255,255,255,255,255,255,255,255,255,
											  255,255,255,255,255,255,255,255,255,255};

				return new HashKey(maxBytes);
			}
		}

		public uint uint1;              
		public uint uint2;              
		public uint uint3;              
		public uint uint4;              
		public uint uint5;                      

		private static Random _rand = new Random();

		public HashKey(uint uint1, uint uint2, uint uint3, uint uint4, uint uint5)
		{
			this.uint1 = uint1;
			this.uint2 = uint2;
			this.uint3 = uint3;
			this.uint4 = uint4;
			this.uint5 = uint5;
		}

		public HashKey(byte[] bytes)
		{
			int dataLength = bytes.Length >> 2;

			int leftOver = bytes.Length & 0x3;

			if (leftOver != 0)
			{
				dataLength++;
			}

			if (dataLength > 5)
			{
				throw (new ArithmeticException("Too many bytes"));
			}

			uint[] temp = new uint[5];

			for (int i = bytes.Length - 1, j = 0; i >= 3; i -= 4, j++)
			{
				temp[j] = (uint)((bytes[i - 3] << 24) + (bytes[i - 2] << 16) + (bytes[i - 1] << 8) + bytes[i]);
			}

			if (leftOver == 1)
			{
				temp[dataLength - 1] = (uint)bytes[0];
			}
			else if (leftOver == 2)
			{
				temp[dataLength - 1] = (uint)((bytes[0] << 8) + bytes[1]);
			}
			else if (leftOver == 3)
			{
				temp[dataLength - 1] = (uint)((bytes[0] << 16) + (bytes[1] << 8) + bytes[2]);
			}
			
			uint1 = temp[0];
			uint2 = temp[1];
			uint3 = temp[2];
			uint4 = temp[3];
			uint5 = temp[4];
		}

		public HashKey(byte[] inData, int inLen)
		{
			int dataLength = inLen >> 2;

			int leftOver = inLen & 0x3;

			if (leftOver != 0)
			{
				dataLength++;
			}

			if (dataLength > 5 || inLen > inData.Length)
			{
				throw (new ArithmeticException("Byte overflow in constructor."));
			}

			uint[] temp = new uint[5];

			for (int i = inLen - 1, j = 0; i >= 3; i -= 4, j++)
			{
				temp[j] = (uint)((inData[i - 3] << 24) + (inData[i - 2] << 16) + (inData[i - 1] << 8) + inData[i]);
			}

			if (leftOver == 1)
			{
				temp[dataLength - 1] = (uint)inData[0];
			}
			else if (leftOver == 2)
			{
				temp[dataLength - 1] = (uint)((inData[0] << 8) + inData[1]);
			}
			else if (leftOver == 3)
			{
				temp[dataLength - 1] = (uint)((inData[0] << 16) + (inData[1] << 8) + inData[2]);
			}

			uint1 = temp[0];
			uint2 = temp[1];
			uint3 = temp[2];
			uint4 = temp[3];
			uint5 = temp[4];
		}

		public byte[] ToBytes()
		{
			byte[] bytes = new byte[20];
			
			uint[] temp = new uint[] {uint1,uint2,uint3,uint4,uint5};

			int dataLength = 5;

			while (dataLength > 1 && temp[dataLength - 1] == 0)
			{
				dataLength--;
			}

			for (int i = 0, j = dataLength - 1; j >= 0; i += 4, j--)
			{
				bytes[i]     = (byte)(temp[j] >> 24);
				bytes[i + 1] = (byte)(temp[j] >> 16);
				bytes[i + 2] = (byte)(temp[j] >> 8);
				bytes[i + 3] = (byte)temp[j];
			}

			return bytes;
		}

		public static bool operator ==(HashKey key1, HashKey key2)
		{
			return key1.Equals(key2);
		}

		public static bool operator !=(HashKey key1, HashKey key2)
		{
			return !(key1.Equals(key2));
		}

		public override bool Equals(object o)
		{
			HashKey key = (HashKey)o;

			if (key.uint1 != uint1 || key.uint2 != uint2 || key.uint3 != uint3 || key.uint4 != uint4 || key.uint5 != uint5)
			{
				return false;
			}

			return true;
		}

		public override int GetHashCode()
		{
			return (int)((uint1 & 0xFF000000) |
						 (uint2 & 0x00FF0000) |
						 (uint3 & 0x0000FF00) |
						 (uint4 & 0x000000FF));
		}

		public int GetHashCode(int which)
		{
			switch (which)
			{
				case 1:     return (int)uint1;
				case 2:     return (int)uint2;
				case 3:     return (int)uint3;
				case 4:     return (int)uint4;
				case 5:     return (int)uint5;
			}

			throw new ArithmeticException();
		}

		public static bool operator >(HashKey key1, HashKey key2)
		{
			return key1.CompareTo(key2) == 1;
		}

		public static bool operator <(HashKey key1, HashKey key2)
		{
			return key1.CompareTo(key2) == -1;
		}

		public static bool operator >=(HashKey key1, HashKey key2)
		{
			return key1.CompareTo(key2) != -1;
		}

		public static bool operator <=(HashKey key1, HashKey key2)
		{
			return key1.CompareTo(key2) != 1;
		}
		
		public override string ToString()
		{
			int dataLength = 5;
			uint[] temp = new uint[] { uint1, uint2, uint3, uint4, uint5 };

			while (dataLength > 1 && temp[dataLength - 1] == 0)
			{
				dataLength--;
			}

			string result = temp[dataLength - 1].ToString("X");

			for (int i = dataLength - 2; i >= 0; i--)
			{
				result += temp[i].ToString("X8");
			}

			return result;
		}
	   
		public void GenerateRandomKey()
		{
			lock (_rand)
			{
				uint1 = (uint)_rand.Next();
				uint2 = (uint)_rand.Next();
				uint3 = (uint)_rand.Next();
				uint4 = (uint)_rand.Next();
				uint5 = (uint)_rand.Next();
			}
		}

		public static HashKey Hash(string plainText)
		{
			SHA1 sha         = SHA1.Create();
			byte[] srcBytes  = ASCIIEncoding.ASCII.GetBytes(plainText);
			byte[] hashBytes = sha.ComputeHash(srcBytes);
			sha.Clear();

			return new HashKey(hashBytes);
		}

		public static HashKey Hash(byte[] srcBytes)
		{
			SHA1 sha         = SHA1.Create();
			byte[] hashBytes = sha.ComputeHash(srcBytes);
			sha.Clear();
			return new HashKey(hashBytes);
		}

		#region IComparable Members

		public int CompareTo(object obj)
		{
			if (obj == null)
			{
				return 1;
			}
	  
			return this.CompareTo((HashKey)obj);
		}
		
		#endregion

		#region IComparable<HashKey> Members

		public int CompareTo(HashKey other)
		{
			if (uint5 > other.uint5)
			{
				return 1;
			}
			else if (uint5 < other.uint5)
			{
				return -1;
			}
			else if (uint4 > other.uint4)
			{
				return 1;
			}
			else if (uint4 < other.uint4)
			{
				return -1;
			}
			else if (uint3 > other.uint3)
			{
				return 1;
			}
			else if (uint3 < other.uint3)
			{
				return -1;
			}
			else if (uint2 > other.uint2)
			{
				return 1;
			}
			else if (uint2 < other.uint2)
			{
				return -1;
			}
			else if (uint1 > other.uint1)
			{
				return 1;
			}
			else if (uint1 < other.uint1)
			{
				return -1;
			}
			else
			{
				return 0;
			}
		}

		#endregion
	}
}
