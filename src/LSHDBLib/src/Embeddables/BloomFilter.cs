using System;
using System.Collections.Generic;
using System.Globalization;
using System.Numerics;
using System.Security.Cryptography;
using System.Text;
using LSHDBLib.Base;

namespace LSHDBLib.Embeddables {
    [Serializable]
    public class BloomFilter : Embeddable {
        //static Charset charset = Charset.forName("UTF-8"); // encoding used for storing hash values as strings
        public BitSet bitset;
        public int bitSetSize;
        public int bitsSet = 0;
        public int numberOfAddedElements; // number of elements actually added to the Bloom filter
        public int k; // number of hash functions
        public int grams;
        public int[] cols;
        public long[] a;
        public long[] b;
        public BloomFilter(){

        }
        public BloomFilter (int length, int k, int grams) {
            this.bitSetSize = length;
            this.bitset = new BitSet (bitSetSize);
            this.cols = new int[bitSetSize];
            this.bitsSet = 0;
            this.k = k;
            a = new long[this.k];
            b = new long[this.k];
            this.grams = grams;
        }
        public void embed (Object v) {
            String s = (String) v;
            encode (s, true);
        }
        public int getSize () {
            return this.bitSetSize;
        }

        public Embeddable freshCopy () {
            return new BloomFilter (this.bitSetSize, this.k, this.grams);
        }

        public BloomFilter (String s, int length, int k, int grams):
            this (length, k, grams) {
                encode (s, true);
            }

        public BloomFilter (List<String> s, int length, int k, int grams):
            this (length, k, grams) {
                encode (s, true);
            }

        public void addElement (String s) {
            //word=binascii.a2b_qp(qgram) # convert to binary

            String mykey = "zuxujesw";
            byte[] keyBytes = Encoding.UTF8.GetBytes (mykey);
            String hex1 = "";
            String hex2 = "";
            try {
                HMACSHA1 mac = new HMACSHA1 (keyBytes);
                mac.Initialize ();
                byte[] inputBytes = new byte[s.Length];
                for(var i=0;i<s.Length;i++){
                    inputBytes.SetValue(Convert.ToByte(s[i]),i);//Encoding.UTF8.GetBytes(s);
                }
                byte[] digest = mac.TransformFinalBlock(inputBytes, 0, inputBytes.Length);
                hex1 = BitConverter.ToString(digest).Replace("-", string.Empty);
            } catch (Exception ex) {
                Console.Error.WriteLine ("HMACSHA1: Hash error: " + ex.Message);
            }
            try {
                HMACMD5 mac = new HMACMD5 (keyBytes);
                mac.Initialize ();
                byte[] inputBytes = Encoding.UTF8.GetBytes (s);
                byte[] digest = mac.TransformFinalBlock (inputBytes, 0, inputBytes.Length);
                hex2 = BitConverter.ToString(digest).Replace("-", string.Empty);
            } catch (Exception ex) {
                Console.Error.WriteLine ("HMACMD5: Hash error: " + ex.Message);
            }
            // convert hash key to integer
            BigInteger h1 = BigInteger.Parse (hex1, NumberStyles.AllowHexSpecifier);
            BigInteger h2 = BigInteger.Parse (hex2, NumberStyles.AllowHexSpecifier);

            for (int i = 0; i < k; i++) {
                BigInteger bigi = new BigInteger (i);
                //BigInteger res = h2.multiply(bigi).add(h1).mod(new BigInteger(this.bitSetSize + ""));
                BigInteger res = (h2 * bigi + h1) % new BigInteger (this.bitSetSize);
                int position = (int) res;
                if (!bitset.get (position)) {
                    bitsSet++;
                }
                if (bitset.get (position)) {
                    if (cols[position] == 0) {
                        cols[position] = 1;
                    } else {
                        cols[position] = cols[position] + 1;
                    }
                }
                bitset.set (position);
            }
            numberOfAddedElements++;
        }
        public String toString () {
            StringBuilder s = new StringBuilder ();
            for (int i = 0; i < this.bitSetSize; i++) {
                if (bitset.get (i)) {
                    s.Append ("1");
                } else {
                    s.Append ("0");
                }
            }
            return s.ToString ();
        }

        public int[] toInt () {
            int[] s = new int[this.bitSetSize];
            for (int i = 0; i < this.bitSetSize; i++) {
                if (bitset.get (i)) {
                    s[i] = 1;
                } else {
                    s[i] = 0;
                }
            }
            return s;
        }

        public int countZeros () {
            int s = 0;
            for (int i = 0; i < this.bitSetSize; i++) {
                if (!bitset.get (i)) {
                    s = s + 1;
                }
            }
            return s;
        }

        public HashSet<int> toSet () {
            HashSet<int> s = new HashSet<int> ();
            for (int i = 0; i < this.bitSetSize; i++) {
                if (bitset.get (i)) {
                    s.Add (i);
                }
            }
            return s;
        }

        public HashSet<int> toSet0 () {
            HashSet<int> s = new HashSet<int> ();
            for (int i = 0; i < this.bitSetSize; i++) {
                if (!bitset.get (i)) {
                    s.Add (i);
                }
            }
            return s;
        }
        public bool getBit (int bit) {
            return bitset.get (bit);
        }

        public void setBit (int bit, bool value) {
            bitset.set (bit, value);
        }
        public BitSet getBitSet () {
            return bitset;
        }
        public int size () {
            return this.bitSetSize;
        }
        public int count () {
            return this.numberOfAddedElements;
        }
        public void encode (String s, bool padded) {
            if (padded) {
                s = "_" + s + "_";
            }
            List<String> ngrams = NGram.getGrams (s, this.grams);
            foreach (String gram in ngrams) {
                addElement (gram);
            }
        }
        public void encode (List<String> numbers, bool padded) {
            foreach (String number in numbers) {
                addElement (number);
            }
        }
        public static BitSet toBitSet (String bf) {
            BitSet bs = new BitSet (bf.Length);
            for (int i = 0; i < bf.Length; i++) {
                if (bf[i] == '1') {
                    bs.set (i);
                }
            }
            return bs;
        }
    }
}