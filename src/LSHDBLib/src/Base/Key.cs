using System;

namespace LSHDBLib.Base {
    public abstract class Key {

        public static String KEYFIELD = "_keyField_";
        public static String TOKENS = "_tokens";
        public int L { get; internal set; }
        public int k { get; internal set; }
        public int size { get; internal set; }
        public double delta { get; internal set; }
        public string keyFieldName { get; internal set; }
        public bool tokenized { get; internal set; }
        public bool performComparisons { get; internal set; }
        public double thresholdRatio { get; internal set; }
        public Embeddable emb { get; internal set; }
        public abstract int optimizeL();
        public abstract Key create (double thresholdRatio);
    }
}