using System;

namespace LSHDBLib.Base {
    public interface Iterable {
        void seek (String partialKey);
        bool hasNext ();
        void next ();
        String getKey ();
        Object getValue ();
        void close ();
    }
}