using LSHDBLib.Base;

namespace LSHDBLib.Embeddables
{
    public class BloomFilter : Embeddable
    {
        private int v1;
        private int v2;
        private int v3;

        public BloomFilter(int v1, int v2, int v3)
        {
            this.v1 = v1;
            this.v2 = v2;
            this.v3 = v3;
        }

        public void embed(object v)
        {
            throw new System.NotImplementedException();
        }

        public Embeddable freshCopy()
        {
            throw new System.NotImplementedException();
        }

        public int getSize()
        {
            throw new System.NotImplementedException();
        }
    }
}