using System;
using LSHDBLib.Base;
using LSHDBLib.Embeddables;

namespace LSHDBLib.Hamming
{
    public class HammingLSHStoreFactory : IDataStoreFactory
    {
        public DataStore build(string folder, string storeName, IStoreEngineFactory dbEngine, Configuration conf, bool massInsertMode)
        {
            return new HammingLSHStore(folder, storeName, dbEngine, conf, massInsertMode);
        }
    }
    public class HammingLSHStore : DataStore
    {
        HammingConfiguration hConf;
        private readonly String folder;
        private readonly String storeName;
        private readonly bool massInsertMode;

        public HammingLSHStore(String folder, String dbName, IStoreEngineFactory dbEngine, Configuration hc, bool massInsertMode)
        {
            this.folder = folder;
            this.storeName = dbName;
            this.massInsertMode = massInsertMode;
            if (hc == null)
            {
                IStoreEngine db = dbEngine.createInstance(folder,dbName, "conf", massInsertMode);
                hConf = new HammingConfiguration(folder, dbName, db, massInsertMode);
            }
            else
            {
                hConf = (HammingConfiguration)hc;
            }
            init(dbEngine, massInsertMode);
        }

        public HammingLSHStore(String folder, String dbName, IStoreEngineFactory dbEngine)
        {
            this.folder = folder;
            this.storeName = dbName;

            IStoreEngine db = dbEngine.createInstance(folder,dbName, "conf", massInsertMode);
            hConf = new HammingConfiguration(folder, dbName, db, massInsertMode);
            init(dbEngine, massInsertMode);
            hConf.saveConfiguration();
        }

        public override Configuration getConfiguration()
        {
            return this.hConf;
        }

        public override bool distance(Embeddable struct1, Embeddable struct2, Key key)
        {
            BloomFilter bf1 = (BloomFilter)struct1;
            BloomFilter bf2 = (BloomFilter)struct2;
            int t = ((HammingKey)key).t;

            BitSet bs1 = bf1.getBitSet();
            BitSet bs2 = bf2.getBitSet();
            int d = distance(bs1, bs2);
            return (d <= t);
        }

        public override String buildHashKey(int j, Embeddable emb, String keyFieldName)
        {
            BloomFilter bf = (BloomFilter)emb;
            BitSet bs = bf.getBitSet();
            String hashKey = "";
            Key key = hConf.getKey(keyFieldName);
            for (int k = 0; k < key.k; k++)
            {
                int position = ((HammingKey)key).samples[j][k];
                if (bs.get(position))
                {
                    hashKey = hashKey + "1";
                }
                else
                {
                    hashKey = hashKey + "0";
                }

            }
            return "L" + j + "_" + hashKey;
        }

        public BitSet toBitSet(String bf)
        {
            BitSet bs = new BitSet(bf.Length);
            for (int i = 0; i < bf.Length; i++)
            {
                if (bf[i] == '1')
                {
                    bs.set(i);
                }
            }
            return bs;
        }

        public int distance(BitSet a, BitSet b)
        {
            int d = 0;
            BitSet c = (BitSet)a.clone();
            c = c.xor(b);
            d = c.cardinality();
            return d;
        }
        public void save()
        {

        }
    }
}