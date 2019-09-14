using System;
using System.Linq;

namespace LSHDBLib.Base {
    [Serializable]
    public class BitSet {
        public int _length;
        public bool[] _array;
        public BitSet(){
            
        }
        public BitSet (int length) {
            _length = length;
            _array = new bool[length];
        }
        public bool get (int idx) {
            return _array[idx];
        }
        public void set (int idx) {
            _array[idx] = true;
        }
        public void set (int idx, bool value) {
            _array[idx] = value;
        }
        public BitSet clone(){
            BitSet newBitSet = new BitSet(this._length);
            for(int idx=0;idx<_length;idx++)
            {
                newBitSet.set(idx, _array[idx]);
            }
            return newBitSet;
        }
        public BitSet xor(BitSet xor){
            BitSet largest = this._length > xor._length ? this : xor;
            BitSet smallest = this._length > xor._length ? xor : this;
            BitSet xordBitSet = new BitSet(largest._length);
            for(int idx=0; idx<largest._length; idx++){
                if(idx>=smallest._length){
                    xordBitSet.set(idx, largest.get(idx));
                } else {
                    xordBitSet.set(idx, largest.get(idx) ^ smallest.get(idx));
                }
            }
            return xordBitSet;
        }
        public int cardinality(){
            return this._array.Count((bool b)=>{ return b==true; });
        }
    }
}