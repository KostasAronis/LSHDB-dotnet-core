using System;
using System.Xml.Serialization;

namespace LSHDBLib.Base {
    [Serializable]
    public abstract class Key {

        public static String KEYFIELD = "_keyField_";
        public static String TOKENS = "_tokens";
        public int L { get; set; }
        public int k { get; set; }
        public int size { get; set; }
        public double delta { get; set; }
        public string keyFieldName { get; set; }
        public bool tokenized { get; set; }
        public bool performComparisons { get; set; }
        public double thresholdRatio { get; set; }
        [XmlIgnore()]
        public Embeddable emb { get; set; }
        public abstract int optimizeL();
        public abstract Key create (double thresholdRatio);
    }
}