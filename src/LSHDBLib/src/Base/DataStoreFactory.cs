using System;

namespace LSHDBLib.Base
{
    public interface IDataStoreFactory
    {
        DataStore build(String folder, String storeName, IStoreEngineFactory dbEngine, Configuration conf, bool massInsertMode);
    }
}