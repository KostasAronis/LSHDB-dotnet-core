using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;

namespace LSHDBLib.Base
{
    public abstract class DataStore {
        String folder;
        String storeName;
        IStoreEngine dbEngine;
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
            String pathToDB = System.IO.Path.Combine(folder, storeName);
            File theDir = new File(pathToDB);
            return theDir.exists();
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
            for (int i = 0; i < this.getNodes().size(); i++) {
                Node node = this.getNodes().get(i);
                if (node.alias.equals(alias)) {
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
            this.nodes.add(n);
        }

        public void addLocalStore(DataStore ds) {
            this.localStores.add(ds);
        }

        public DataStore getLocalStore(String storeName) {
            for (int i = 0; i < this.getLocalStores().size(); i++) {
                DataStore ds = this.getLocalStores().get(i);
                if (ds.getStoreName().equals(storeName)) {
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
            keyMap[fieldName] = IStoreEngineFactory.build(folder, storeName, KEYS + "_" + fieldName, dbEngine, massInsertMode));
            cacheMap[fieldName] = new Dictionary<string,Object>());
        }

        public IStoreEngine getDataMap(String fieldName) {
            fieldName = fieldName.replaceAll(" ", "");
            return dataMap.get(fieldName);
        }

        public void setDataMap(String fieldName, bool massInsertMode) throws NoSuchMethodException, ClassNotFoundException {
            fieldName = fieldName.replaceAll(" ", "");
            dataMap.put(fieldName, IStoreEngineFactory.build(folder, storeName, DATA + "_" + fieldName, dbEngine, massInsertMode));
        }

        public void init(String dbEngine, bool massInsertMode) throws StoreInitException {
            try {
                this.dbEngine = dbEngine;
                pathToDB = folder + System.getProperty("file.separator") + storeName;
                records = IStoreEngineFactory.build(folder, storeName, RECORDS, dbEngine, massInsertMode);
                if ((this.getConfiguration() != null) && (this.getConfiguration().isKeyed())) {
                    String[] keyFieldNames = this.getConfiguration().getKeyFieldNames();
                    for (int j = 0; j < keyFieldNames.length; j++) {
                        String keyFieldName = keyFieldNames[j];
                        setKeyMap(keyFieldName, massInsertMode);
                        setDataMap(keyFieldName, massInsertMode);
                    }
                } else {
                    keys = IStoreEngineFactory.build(folder, storeName, KEYS, dbEngine, massInsertMode);
                    data = IStoreEngineFactory.build(folder, storeName, DATA, dbEngine, massInsertMode);
                    keyMap.put(Configuration.RECORD_LEVEL, keys);
                    dataMap.put(Configuration.RECORD_LEVEL, data);
                    cacheMap.put(Configuration.RECORD_LEVEL, new Dictionary());

                }
            } catch (ClassNotFoundException ex) {
                throw new StoreInitException("Declared class " + dbEngine + " not found.");
            } catch (NoSuchMethodException ex) {
                throw new StoreInitException("The particular constructor cannot be found in the decalred class " + dbEngine + ".");
            }
        }

        public void close() {
            hashTablesExecutor.shutdown();
            nodesExecutor.shutdown();

            records.close();
            if (this.getConfiguration().isKeyed()) {
                String[] keyFieldNames = this.getConfiguration().keyFieldNames;
                for (int j = 0; j < keyFieldNames.length; j++) {
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

        public String getDbEngine() {
            return this.dbEngine;
        }

        public Result forkQuery(QueryRecord queryRecord) {
            Result result = new Result(queryRecord);
            //if (this.getNodes().size() == 0) {
            //  return result;
            //}
            // should implment get Active Nodes
            List<Callable<Result>> callables = new ArrayList<Callable<Result>>();

            final QueryRecord q = queryRecord;
            for (int i = 0; i < this.getNodes().size(); i++) {
                final Node node = this.getNodes().get(i);
                if (node.isEnabled()) {
                    callables.add(new Callable<Result>() {
                        public Result call() {
                            Result r = null;
                            if ((!node.isLocal()) && (q.isClientQuery())) {
                                Client client = new Client(node.url, node.port);
                                try {
                                    QueryRecord newQuery = (QueryRecord) q.clone();
                                    newQuery.setServerQuery();
                                    r = client.queryServer(newQuery);
                                    if (r == null) {
                                        r = new Result(newQuery);
                                        r.setStatus(Result.NULL_RESULT_RETURNED);
                                    }
                                    r.setRemote();
                                    r.setOrigin(node.alias);
                                } catch (CloneNotSupportedException | NodeCommunicationException ex) {
                                    if (r == null) {
                                        r = new Result(q);
                                    }
                                    r.setRemote();
                                    r.setOrigin(node.alias);
                                    r.setStatus(Result.NO_CONNECT);
                                }

                            } else if (node.isLocal()) {
                                try {
                                    r = query(q);
                                    r.setStatus(Result.STATUS_OK);
                                    r.prepare();
                                    r.setOrigin(node.alias);
                                } catch (NoKeyedFieldsException ex) {
                                    if (r != null) {
                                        r = new Result(q);
                                    }
                                    r.setOrigin(node.alias);
                                    r.setStatus(Result.NO_KEYED_FIELDS_SPECIFIED);
                                }
                            }
                            return r;
                        }
                    });
                }
            }

            Result partialResults = null;
            try {

                List<Future<Result>> futures = nodesExecutor.invokeAll(callables);

                for (Future<Result> future : futures) {

                    if (future != null) {  //partialResults should not come null

                        partialResults = future.get();
                        if (partialResults != null) {
                            result.getRecords().addAll(partialResults.getRecords());
                            result.setStatus(partialResults.getOrigin(), partialResults.getStatus());
                        }
                    }

                }
            } catch (ExecutionException | InterruptedException ex) {
                if (ex.getCause() != null) {
                    String server = " ";
                    if (partialResults != null) {
                        server = partialResults.getOrigin();
                    }
                    log.error("forkQuery error ", ex);
                    if (ex.getCause() instanceof Error) {
                        log.fatal("forkQuery Fatal error occurred on " + server, ex);
                        Node node = getNode(server);
                        if (node != null) {
                            node.disable();
                        }
                    }

                }
            }
            return result;
        }

        public int getThreadsNo() {
            ThreadMXBean bean = ManagementFactory.getThreadMXBean();
            return bean.getThreadCount();
        }

        public Result forkHashTables(Embeddable struct1, final QueryRecord queryRec, String keyFieldName) {
            final Configuration conf = this.getConfiguration();
            final int maxQueryRows = queryRec.getMaxQueryRows();
            final bool performComparisons = queryRec.performComparisons(keyFieldName);
            final double userPercentageThreshold = queryRec.getUserPercentageThreshold(keyFieldName);
            final IStoreEngine keys = this.getKeyMap(keyFieldName);
            final IStoreEngine data = this.getDataMap(keyFieldName);
            Key key = conf.getKey(keyFieldName);
            bool isPrivateMode = conf.isPrivateMode();

            final String keyFieldName1 = keyFieldName;
            final Embeddable struct11 = struct1;

            final Key newKey = key.create(userPercentageThreshold);
            int partitionsNo = newKey.getL() / NO_FORKED_HASHTABLES;
            if (newKey.getL() % NO_FORKED_HASHTABLES != 0) {
                partitionsNo++;
            }

            Instant start = Instant.now();
            final Result result = new Result(queryRec);

            for (int p = 0; p < partitionsNo; p++) {

                List<Callable<Result>> callables = new ArrayList<Callable<Result>>();
                final int noHashTable = p * NO_FORKED_HASHTABLES;
                callables.add(new Callable<Result>() {
                    public Result call() throws StoreInitException, NoKeyedFieldsException {
                        Iterable iterator = keys.createIterator();
                        int u = noHashTable + NO_FORKED_HASHTABLES;
                        for (int j = noHashTable; j < u; j++) {
                            if (j == newKey.getL()) {
                                break;
                            }
                            String hashKey = buildHashKey(j, struct11, keyFieldName1);

                            //if (keys.contains(hashKey)) {
                            //  ArrayList arr = (ArrayList) keys.get(hashKey);
                            //for (int i = 0; i < arr.size(); i++) {
                            for (iterator.seek(hashKey); iterator.hasNext(); iterator.next()) {
                                String key = iterator.getKey();
                                if (key.startsWith(hashKey)) {
                                    //String id = "";
                                    ArrayList<String> arr = null;
                                    try {
                                        //id = iterator.getValue() + "";
                                        arr = (ArrayList) iterator.getValue();
                                    } catch (Exception ex) {
                                        ex.printStackTrace();
                                    }

                                    CharSequence cSeq = Key.KEYFIELD;

                                    for (int m = 0; m < arr.size(); m++) {
                                        String id = arr.get(m);

                                        String idRec = id;
                                        if (idRec.contains(cSeq)) {
                                            idRec = id.split(Key.KEYFIELD)[0];
                                        }
                                        Record dataRec = null;
                                        if (!conf.isPrivateMode()) {
                                            dataRec = (Record) records.get(idRec);   // which id and which record shoudl strip the "_keyField_" part , if any
                                        } else {
                                            dataRec = new Record();
                                            dataRec.setId(idRec);
                                        }

                                        result.incPairsNo();
                                        if ((performComparisons) && (!result.getMap(keyFieldName1).containsKey(id))) {
                                            Embeddable struct2 = (Embeddable) data.get(id);
                                            if (distance(struct11, struct2, newKey)) {
                                                result.add(keyFieldName1, dataRec);
                                                int matchesNo = result.getDataRecordsSize(keyFieldName1);
                                                if (matchesNo >= maxQueryRows) {
                                                    return result;
                                                }
                                            } else {
                                            }

                                        } else {
                                            result.add(keyFieldName1, dataRec);
                                        }
                                    }
                                } else {
                                    break;
                                }
                            }

                        }
                        iterator.close();
                        return result;
                    }

                });

                try {
                    List<Future<Result>> futures = hashTablesExecutor.invokeAll(callables);
                    Instant end = Instant.now();

                    if (result.getRecords().size() >= maxQueryRows) {
                        throw new MaxNoRecordsReturnedException("Limit of returned records exceeded. No=" + result.getRecords().size());
                    }

                } catch (InterruptedException ex) {
                    log.error("forkHashTables ", ex);
                } catch (MaxNoRecordsReturnedException ex) {

                    return result;
                }

            }

            return result;
        }

        public void persistCache(String keyFieldName) {
            bool isKeyed = this.getConfiguration().isKeyed();
            IStoreEngine hashKeys = keys;
            if (isKeyed) 
                hashKeys = this.getKeyMap(keyFieldName);
            Dictionary<String, ArrayList> cache = getCacheMap(keyFieldName);
            for (Map.Entry<String,ArrayList> entry : cache.entrySet()) {
                long tt = incId();
                //System.out.println("key="+entry.getKey()+ "_" + tt);
                hashKeys.set(entry.getKey() + "_" + tt, entry.getValue());
            }
            cache.clear();
            //cacheMap.put(keyFieldName, new Dictionary());
        }
        
        public void setHashKeys(String id, Embeddable emb, String keyFieldName) {
            bool isKeyed = this.getConfiguration().isKeyed();
            String[] keyFieldNames = this.getConfiguration().getKeyFieldNames();
            IStoreEngine hashKeys = keys;
            if (isKeyed) {
                hashKeys = this.getKeyMap(keyFieldName);
            }
            Dictionary<String, ArrayList> cache = this.getCacheMap(keyFieldName);
            Key key = this.getConfiguration().getKey(keyFieldName);
            
            for (int j = 0; j < key.L; j++) {
                String hashKey = buildHashKey(j, emb, keyFieldName);

                /* ArrayList<String> arr = new ArrayList<String>();
                arr.add(id);
                hashKeys.set(hashKey+"_"+incId(), arr );*/
                
                if (cache.containsKey(hashKey)) {
                    ArrayList arr = cache.get(hashKey);
                    arr.add(id);
                    if (arr.size() >= CACHEENTRYLIMIT) {
                        long tt = incId();
                        hashKeys.set(hashKey + "_" + tt, arr);
                        cache.remove(hashKey);
                    }
                } else {
                    ArrayList<String> arr = new ArrayList<String>();
                    arr.add(id);
                    cache.put(hashKey, arr);
                }
                
                if (cache.size() >= CACHENOLIMIT){
                    persistCache(keyFieldName);
                }
            }
        }

        public void insert(Record rec) {
            if (this.getConfiguration().isPrivateMode()) {
                Embeddable emb = (Embeddable) rec.get(Record.PRIVATE_STRUCTURE);
                data.set(rec.getId(), emb);
                setHashKeys(rec.getId(), emb, Configuration.RECORD_LEVEL);
                return;
            }

            bool isKeyed = this.getConfiguration().isKeyed();
            String[] keyFieldNames = this.getConfiguration().getKeyFieldNames();
            Dictionary<String, ? extends Embeddable[]> embMap = buildEmbeddableMap(rec);

            if (isKeyed) {
                for (int i = 0; i < keyFieldNames.length; i++) {
                    String keyFieldName = keyFieldNames[i];
                    Embeddable[] embs = embMap.get(keyFieldName);
                    for (int j = 0; j < embs.length; j++) {
                        Embeddable emb = embs[j];
                        setHashKeys(rec.getId() + Key.KEYFIELD + j, emb, keyFieldName);
                        this.getDataMap(keyFieldName).set(rec.getId() + Key.KEYFIELD + j, emb);
                    }

                }
            } else {
                data.set(rec.getId(), ((Embeddable[]) embMap.get(Configuration.RECORD_LEVEL))[0]);
                setHashKeys(rec.getId(), ((Embeddable[]) embMap.get(Configuration.RECORD_LEVEL))[0], Configuration.RECORD_LEVEL);
            }

            records.set(rec.getId(), rec);
        }

        public Result query(QueryRecord queryRecord) throws NoKeyedFieldsException {
            Result result = null;
            Configuration conf = this.getConfiguration();
            IStoreEngine hashKeys = keys;
            IStoreEngine dataKeys = data;
            Dictionary<String, ? extends Embeddable[]> embMap = null;
            if (!conf.isPrivateMode()) {
                embMap = buildEmbeddableMap(queryRecord);
            }
            bool isKeyed = this.getConfiguration().isKeyed();
            String[] keyFieldNames = this.getConfiguration().getKeyFieldNames();
            ArrayList<String> fieldNames = queryRecord.getFieldNames();

            if ((fieldNames.isEmpty()) && (conf.isKeyed)) {
                throw new NoKeyedFieldsException(Result.NO_KEYED_FIELDS_SPECIFIED_ERROR_MSG);
            }
            if (ListUtil.intersection(fieldNames, Arrays.asList(keyFieldNames)).isEmpty() && (conf.isKeyed)) {
                throw new NoKeyedFieldsException(Result.NO_KEYED_FIELDS_SPECIFIED_ERROR_MSG);
            }

            for (int i = 0; i < fieldNames.size(); i++) {
                String fieldName = fieldNames.get(i);
                if (keyFieldNames != null) {
                    for (int j = 0; j < keyFieldNames.length; j++) {
                        String keyFieldName = keyFieldNames[j];
                        if (keyFieldName.equals(fieldName)) {
                            Embeddable[] structArr = embMap.get(fieldName);
                            for (int k = 0; k < structArr.length; k++) {
                                result = forkHashTables(structArr[k], queryRecord, keyFieldName);

                            }
                        }
                    }
                }

            }

            if (!isKeyed) {
                Embeddable emb = null;
                if (conf.isPrivateMode()) {
                    emb = (Embeddable) queryRecord.get(Record.PRIVATE_STRUCTURE);
                } else {
                    emb = ((Embeddable[]) embMap.get(Configuration.RECORD_LEVEL))[0];
                }
                result = forkHashTables(emb, queryRecord, Configuration.RECORD_LEVEL);
            }

            return result;
        }

        public Dictionary<String, Embeddable[]> buildEmbeddableMap(Record rec) {

            Dictionary<String, Embeddable[]> embMap = new Dictionary<String, Embeddable[]>();
            bool isKeyed = this.getConfiguration().isKeyed();
            String[] keyFieldNames = this.getConfiguration().getKeyFieldNames();
            ArrayList<String> fieldNames = rec.getFieldNames();
            Embeddable embRec = null;
            if ((!isKeyed) && (this.getConfiguration().getKey(Configuration.RECORD_LEVEL) != null)) {
                embRec = this.getConfiguration().getKey(Configuration.RECORD_LEVEL).getEmbeddable().freshCopy();
            }

            for (int i = 0; i < fieldNames.size(); i++) {
                String fieldName = fieldNames.get(i);
                bool isNotIndexedField = rec.isNotIndexedField(fieldName);
                String s = (String) rec.get(fieldName);
                if (isKeyed) {
                    for (int j = 0; j < keyFieldNames.length; j++) {
                        String keyFieldName = keyFieldNames[j];
                        if (keyFieldName.equals(fieldName)) {
                            Key key = this.getConfiguration().getKey(keyFieldName);
                            bool isTokenized = key.isTokenized();
                            if (!isTokenized) {
                                Embeddable emb = key.getEmbeddable().freshCopy();
                                emb.embed(s);
                                embMap.put(keyFieldName, new Embeddable[]{emb});
                            } else {
                                String[] keyValues = (String[]) rec.get(keyFieldName + Key.TOKENS);
                                Embeddable[] bfs = new Embeddable[keyValues.length];
                                for (int k = 0; k < bfs.length; k++) {
                                    String v = keyValues[k];
                                    Embeddable emb = key.getEmbeddable().freshCopy();
                                    emb.embed(v);
                                    bfs[k] = emb;
                                }
                                embMap.put(keyFieldName, bfs);
                            }
                        }
                    }
                } else if (!isNotIndexedField) {
                    if (embRec != null) {
                        embRec.embed(s);
                    } else {
                        log.error("Although no key fields are specified, a record-level embeddable is missing.");
                    }
                }
            }
            if (!isKeyed) {
                embMap.put(Configuration.RECORD_LEVEL, new Embeddable[]{embRec});
            }

            return embMap;
        }

        /*
        * Opens a HammingLSH store
        * found in specified @target.
        * @throws StoreInitExcoetion
        */
        public static DataStore open(String storeName){
            Config conf = new Config(Config.CONFIG_FILE);
            StoreConfigurationParams c = conf.get(Config.CONFIG_STORE, storeName);
            if (c != null) {
                try {
                    DataStore ds = DataStoreFactory.build(c.getTarget(), storeName, c.getLSHStore(), c.getEngine(), null, true);
                    return ds;
                } catch (ClassNotFoundException | NoSuchMethodException ex) {
                    log.error("Initialization error of data store " + storeName, ex);
                }
            }
            throw new StoreInitException("store " + storeName + " not initialized. Check config.xml ");
        }

        public abstract String buildHashKey(int j, Embeddable struct, String keyFieldName);

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