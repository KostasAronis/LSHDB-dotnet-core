namespace LSHDBLib.Base
{
    public class BitSet
    {
        private int _length;
        private bool[] _array;

        public BitSet(int length){
            _length = length;
            _array = new bool[length];
        }
        public bool get(int idx){
            return _array[idx];
        }
        public void set(int idx){
            _array[idx]=true;
        }
        public void set(int idx, bool value){
            _array[idx]=value;
        }
    }
}