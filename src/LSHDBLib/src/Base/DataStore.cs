using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using LSHDBLib.Base;
using LSHDBLib.Result;

namespace LSHDBLib.Base
{
    public abstract class DataStore {
        String folder;
        String storeName;
        IStoreEngineFactory dbEngine;
        IDataStoreFactory dataStoreFactory;
        public String pathToDB;
        public IStoreEngine data;
        public IStoreEngine keys;
        public IStoreEngine records;
        Dictionary<String, IStoreEngine> keyMap = new Dictionary<String, IStoreEngine>();
        Dictionary<String, Dictionary<String,Object>> cacheMap = new Dictionary<String, Dictionary<String,Object>>();
        Dictionary<String, IStoreEngine> dataMap = new Dictionary<String, IStoreEngine>();
        List<Node> nodes = new List<Node>();
        List<DataStore> localStores = new List<DataStore>();
        bool queryRemoteNodes = false;
        bool massInsertMode = false;

        public static String KEYS = "keys";
        public static String DATA = "data";
        public static String CONF = "conf";
        public static String RECORDS = "records";
        public static int NO_FORKED_HASHTABLES = 10;

        //TODO: concurrency using threadpools or equivelant
        /*
        *   private ExecutorService hashTablesExecutor = Executors.newFixedThreadPool(600);
        *   private ExecutorService nodesExecutor = Executors.newCachedThreadPool();
        */
        private long next = 0;
        private int CACHEENTRYLIMIT = 5000;
        private int CACHENOLIMIT = 60000;
        
        public void setCacheEntryLimit(int limit) {
            this.CACHEENTRYLIMIT = limit;
        }

        public int getCacheEntryLimit() {
            return this.CACHEENTRYLIMIT;
        }
        
        public void setCacheNoLimit(int limit) {
            this.CACHENOLIMIT = limit;
        }

        private readonly object incIdLock = new object();
        public long incId() {
            lock(incIdLock){
                next++;
                long tt = CurrentTimeMillis() + next;
                return tt;
            }
        }
        public static bool exists(String folder, String storeName) {
            return File.Exists(Path.Combine(folder, storeName));
        }
        public void setMassInsertMode(bool status) {
            massInsertMode = status;
        }
        public bool getMassInsertMode() {
            return massInsertMode;
        }
        public void setQueryMode(bool status) {
            queryRemoteNodes = status;
        }
        public bool getQueryMode() {
            return queryRemoteNodes;
        }
        public Node getNode(String alias) {
            for (int i = 0; i < this.getNodes().Count; i++) {
                Node node = this.getNodes()[i];
                if (node.alias.Equals(alias)) {
                    return node;
                }
            }
            return null;
        }
        public List<Node> getNodes() {
            return this.nodes;
        }
        public List<DataStore> getLocalStores() {
            return this.localStores;
        }

        public void addNode(Node n) {
            this.nodes.Add(n);
        }

        public void addLocalStore(DataStore ds) {
            this.localStores.Add(ds);
        }

        public DataStore getLocalStore(String storeName) {
            for (int i = 0; i < this.getLocalStores().Count; i++) {
                DataStore ds = this.getLocalStores()[i];
                if (ds.getStoreName().Equals(storeName)) {
                    return ds;
                }
            }
            return null;
        }

        public IStoreEngine getKeyMap(String fieldName) {
            fieldName = fieldName.Replace(" ", "");
            return keyMap[fieldName];
        }

        public Dictionary<string,Object> getCacheMap(String fieldName) {
            fieldName = fieldName.Replace(" ", "");
            return cacheMap[fieldName];
        }

        public void setKeyMap(String fieldName, bool massInsertMode){
            fieldName = fieldName.Replace(" ", "");
            keyMap[fieldName] = dbEngine.createInstance(folder, storeName, KEYS + "_" + fieldName, massInsertMode);
            cacheMap[fieldName] = new Dictionary<string,Object>();
        }

        public IStoreEngine getDataMap(String fieldName) {
            fieldName = fieldName.Replace(" ", "");
            return dataMap[fieldName];
        }

        public void setDataMap(String fieldName, bool massInsertMode){
            fieldName = fieldName.Replace(" ", "");
            dataMap[fieldName] = dbEngine.createInstance(folder, storeName, DATA + "_" + fieldName, massInsertMode);
        }

        public void init(IStoreEngineFactory dbEngine, bool massInsertMode){
            try {
                this.dbEngine = dbEngine;
                pathToDB = Path.Combine(folder, storeName);
                records = dbEngine.createInstance(folder, storeName, RECORDS, massInsertMode);
                if ((this.getConfiguration() != null) && (this.getConfiguration().isKeyed)) {
                    String[] keyFieldNames = this.getConfiguration().keyFieldNames;
                    for (int j = 0; j < keyFieldNames.Length; j++) {
                        String keyFieldName = keyFieldNames[j];
                        setKeyMap(keyFieldName, massInsertMode);
                        setDataMap(keyFieldName, massInsertMode);
                    }
                } else {
                    keys = dbEngine.createInstance(folder, storeName, KEYS, massInsertMode);
                    data = dbEngine.createInstance(folder, storeName, DATA, massInsertMode);
                    keyMap[Configuration.RECORD_LEVEL] = keys;
                    dataMap[Configuration.RECORD_LEVEL] = data;
                    cacheMap[Configuration.RECORD_LEVEL] = new Dictionary<String, Object>();

                }
            } catch (Exception ex) {
                throw new Exception("Something went wrong while creating the dbEngine");
            }
        }

        public void close() {
            // TODO: when concurrency is implemented a lot of work needs to be done
            // hashTablesExecutor.shutdown();
            // nodesExecutor.shutdown();

            records.close();
            if (this.getConfiguration().isKeyed) {
                String[] keyFieldNames = this.getConfiguration().keyFieldNames;
                for (int j = 0; j < keyFieldNames.Length; j++) {
                    String keyFieldName = keyFieldNames[j];
                    persistCache(keyFieldName);
                    IStoreEngine keyStore = getKeyMap(keyFieldName);
                    keyStore.close();
                    IStoreEngine dataStore = getDataMap(keyFieldName);
                    dataStore.close();
                }
            } else {
                data.close();
                keys.close();
            }
        }

        public String getStoreName() {
            return this.storeName;
        }

        public IStoreEngineFactory getDbEngine() {
            return this.dbEngine;
        }

        public void persistCache(String keyFieldName) {
            bool isKeyed = this.getConfiguration().isKeyed;
            IStoreEngine hashKeys = keys;
            if (isKeyed) 
                hashKeys = this.getKeyMap(keyFieldName);
            Dictionary<String, Object> cache = getCacheMap(keyFieldName);
            foreach (string key in cache.Keys)
            {
                long tt = incId();
                hashKeys.set(key + "_" + tt, cache[key]);
            }
            cache.Clear();
        }
        
        public void setHashKeys(String id, Embeddable emb, String keyFieldName) {
            bool isKeyed = this.getConfiguration().isKeyed;
            String[] keyFieldNames = this.getConfiguration().keyFieldNames;
            IStoreEngine hashKeys = keys;
            if (isKeyed) {
                hashKeys = this.getKeyMap(keyFieldName);
            }
            Dictionary<String, Object> cache = this.getCacheMap(keyFieldName);
            Key key = this.getConfiguration().getKey(keyFieldName);
            
            for (int j = 0; j < key.L; j++) {
                String hashKey = buildHashKey(j, emb, keyFieldName);

                if (cache.ContainsKey(hashKey)) {
                    List<String> arr = (List<String>)cache[hashKey];
                    arr.Add(id);
                    if (arr.Count >= CACHEENTRYLIMIT) {
                        long tt = incId();
                        hashKeys.set(hashKey + "_" + tt, arr);
                        cache.Remove(hashKey);
                    }
                } else {
                    List<String> arr = new List<String>();
                    arr.Add(id);
                    cache[hashKey] = arr;
                }
                
                if (cache.Count >= CACHENOLIMIT){
                    persistCache(keyFieldName);
                }
            }
        }

        public void insert(Record rec) {
            if (this.getConfiguration().isPrivate) {
                Embeddable emb = (Embeddable) rec.get(Record.PRIVATE_STRUCTURE);
                data.set(rec.getId(), emb);
                setHashKeys(rec.getId(), emb, Configuration.RECORD_LEVEL);
                return;
            }

            bool isKeyed = this.getConfiguration().isKeyed;
            String[] keyFieldNames = this.getConfiguration().keyFieldNames;
            Dictionary<String, Embeddable[]> embMap = buildEmbeddableMap(rec);

            if (isKeyed) {
                for (int i = 0; i < keyFieldNames.Length; i++) {
                    String keyFieldName = keyFieldNames[i];
                    Embeddable[] embs = embMap[keyFieldName];
                    for (int j = 0; j < embs.Length; j++) {
                        Embeddable emb = embs[j];
                        setHashKeys(rec.getId() + Key.KEYFIELD + j, emb, keyFieldName);
                        this.getDataMap(keyFieldName).set(rec.getId() + Key.KEYFIELD + j, emb);
                    }

                }
            } else {
                data.set(rec.getId(), ((Embeddable[]) embMap[Configuration.RECORD_LEVEL])[0]);                
                setHashKeys(rec.getId(), ((Embeddable[]) embMap[Configuration.RECORD_LEVEL])[0], Configuration.RECORD_LEVEL);
            }

            records.set(rec.getId(), rec);
        }

        public Dictionary<String, Embeddable[]> buildEmbeddableMap(Record rec) {

            Dictionary<String, Embeddable[]> embMap = new Dictionary<String, Embeddable[]>();
            bool isKeyed = this.getConfiguration().isKeyed;
            String[] keyFieldNames = this.getConfiguration().keyFieldNames;
            List<String> fieldNames = rec.getFieldNames();
            Embeddable embRec = null;
            if ((!isKeyed) && (this.getConfiguration().getKey(Configuration.RECORD_LEVEL) != null)) {
                embRec = this.getConfiguration().getKey(Configuration.RECORD_LEVEL).emb.freshCopy();
            }

            for (int i = 0; i < fieldNames.Count; i++) {
                String fieldName = fieldNames[i];
                bool isNotIndexedField = rec.isNotIndexedField(fieldName);
                String s = (String) rec.get(fieldName);
                if (isKeyed) {
                    for (int j = 0; j < keyFieldNames.Length; j++) {
                        String keyFieldName = keyFieldNames[j];
                        if (keyFieldName.Equals(fieldName)) {
                            Key key = this.getConfiguration().getKey(keyFieldName);
                            bool isTokenized = key.tokenized;
                            if (!isTokenized) {
                                Embeddable emb = key.emb.freshCopy();
                                emb.embed(s);
                                embMap[keyFieldName] = new Embeddable[]{emb};
                            } else {
                                String[] keyValues = (String[]) rec.get(keyFieldName + Key.TOKENS);
                                Embeddable[] bfs = new Embeddable[keyValues.Length];
                                for (int k = 0; k < bfs.Length; k++) {
                                    String v = keyValues[k];
                                    Embeddable emb = key.emb.freshCopy();
                                    emb.embed(v);
                                    bfs[k] = emb;
                                }
                                embMap[keyFieldName] = bfs;
                            }
                        }
                    }
                } else if (!isNotIndexedField) {
                    if (embRec != null) {
                        embRec.embed(s);
                    } else {
                        Console.Error.WriteLine("Although no key fields are specified, a record-level embeddable is missing.");
                    }
                }
            }
            if (!isKeyed) {
                embMap[Configuration.RECORD_LEVEL] = new Embeddable[]{embRec};
            }
            return embMap;
        }

        /*
        * Opens a HammingLSH store
        * found in specified @target.
        * @throws StoreInitExcoetion
        */
        // public static DataStore open(String storeName){
        //     // Config conf = new Config(Config.CONFIG_FILE);
        //     // StoreConfigurationParams c = conf.get(Config.CONFIG_STORE, storeName);
        //     try {
        //         DataStore ds = this.dataStoreFactory.create(c.getTarget(), storeName, c.getLSHStore(), c.getEngine(), null, true);
        //         return ds;
        //     } catch (Exception ex) {
        //         Console.Error.WriteLine("Initialization error of data store " + storeName, ex);
        //         return null;
        //     }
        // }

        public abstract String buildHashKey(int j, Embeddable structure, String keyFieldName);

        public abstract bool distance(Embeddable struct1, Embeddable struct2, Key key);

        public abstract Configuration getConfiguration();
        private static readonly DateTime Jan1st1970 = new DateTime
            (1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        public static long CurrentTimeMillis()
        {
            return (long) (DateTime.UtcNow - Jan1st1970).TotalMilliseconds;
        }
    }
}