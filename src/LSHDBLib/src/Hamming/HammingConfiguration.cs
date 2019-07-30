using System;
using System.Runtime.Serialization;
using LSHDBLib.Base;

namespace LSHDBLib.Hamming {
    public class HammingConfiguration : Configuration {
        public HammingConfiguration (String folder, String dbName, IStoreEngine dbEngine, bool massInsertMode):
            base (folder, dbName, dbEngine, massInsertMode) {

            }

        public HammingConfiguration (String folder, String dbName, IStoreEngine dbEngine, Key[] keysList, bool massInsertMode):
            base (folder, dbName, dbEngine, keysList, massInsertMode) {

            }
    }
}