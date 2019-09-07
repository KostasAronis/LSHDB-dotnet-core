using System;
using System.Runtime.Serialization;
using LSHDBLib.Base;

namespace LSHDBLib.Hamming {
    public class HammingConfigurationFactory : ConfigurationFactory
    {
        public Configuration create(string folder, string dbName, IStoreEngine dbEngine, bool massInsertMode)
        {
            return new HammingConfiguration(folder, dbName, dbEngine, massInsertMode);
        }

        public Configuration create(string folder, string dbName, IStoreEngine db, Key[] keysList, bool massInsertMode)
        {
            return new HammingConfiguration(folder, dbName, db, keysList, massInsertMode);
        }
    }
    public class HammingConfiguration : Configuration {
        public HammingConfiguration (String folder, String dbName, IStoreEngine dbEngine, bool massInsertMode):
            base (folder, dbName, dbEngine, massInsertMode) {

            }

        public HammingConfiguration (String folder, String dbName, IStoreEngine dbEngine, Key[] keysList, bool massInsertMode):
            base (folder, dbName, dbEngine, keysList, massInsertMode) {

            }
    }
}