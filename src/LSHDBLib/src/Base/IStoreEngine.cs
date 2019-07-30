using System;
using System.Collections;

namespace LSHDBLib.Base
{
    public enum Engine{

    }
    public interface IStoreEngine {
    
        void set(String key, Object data);
        Object get(String key);
        bool contains(String key);
        long count();
        void close();
        IEnumerable createIterator();
    }
}