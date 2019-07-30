using System;
using System.Collections.Generic;

namespace LSHDBLib.Base {
    public abstract class Configuration {
        public static String RECORD_LEVEL = "recordLevel";
        public static String PRIVATE_MODE = "privateLevel";
        public static String KEY_NAMES = "keyFieldNames";
        public static String KEY_MODE = "isKeyed";

        String folder;
        String dbName;
        public IStoreEngine db { get; internal set; }
        String[] keyFieldNames;
        public bool isKeyed { get; internal set; } = false;
        bool _isPrivate = false;
        public bool isPrivate {
            get {
                return _isPrivate;
            }
            set {
                _isPrivate = value;
                db.set (PRIVATE_MODE, value);
            }
        }
        Dictionary<String, Key> keys = new Dictionary<String, Key> ();
        public Key getKey (String key) {
            Key val;
            if (keys.TryGetValue (key, out val)) {
                return val;
            }
            return null;
        }

        public void saveConfiguration () {
            close ();
        }

        public void close () {
            db.close ();
        }

        //TODO: StoreEngineFactory has to go, the engine should be injected directly.
        public Configuration (String folder, String dbName, IStoreEngine db, bool massInsertMode) {
            try {
                this.folder = folder;
                this.dbName = dbName;
                this.db=db;
                //db = StoreEngineFactory.build(folder, dbName, "conf", dbEngine, massInsertMode);
                if (db.contains (Configuration.KEY_NAMES)) {
                    this.keyFieldNames = (String[]) db.get (Configuration.KEY_NAMES);
                }
                if (db.contains (Configuration.KEY_MODE)) {
                    this.isKeyed = (bool) db.get (Configuration.KEY_MODE);
                }
                if (keyFieldNames != null) {
                    for (int i = 0; i < this.keyFieldNames.Length; i++) {
                        String keyFieldName = this.keyFieldNames[i];
                        keys[keyFieldName] = (Key) db.get ("conf_" + keyFieldName);
                    }
                }
            } catch (Exception ex) {
                throw new StoreInitException ("Store init error: " + ex.Message);
            }
        }

        public Configuration (String folder, String dbName, IStoreEngine db, Key[] keysList, bool massInsertMode) {
            try {
                this.folder = folder;
                this.dbName = dbName;
                this.db=db;
                //db = StoreEngineFactory.build(folder, dbName, "conf", dbEngine, massInsertMode);
                if (db.contains (Configuration.KEY_NAMES)) {
                    this.keyFieldNames = (String[]) db.get (Configuration.KEY_NAMES);
                    for (int i = 0; i < this.keyFieldNames.Length; i++) {
                        String keyFieldName = this.keyFieldNames[i];
                        this.keys[keyFieldName] = (Key) db.get ("conf_" + keyFieldName);
                    }
                    if (db.contains (Configuration.KEY_MODE)) {
                        this.isKeyed = (bool) db.get (Configuration.KEY_MODE);
                    }
                    if (db.contains (Configuration.PRIVATE_MODE)) {
                        this.isPrivate = true;
                    }

                } else {
                    this.keyFieldNames = new String[keysList.Length];
                    for (int i = 0; i < keysList.Length; i++) {
                        this.keyFieldNames[i] = keysList[i].keyFieldName;
                        keys[keyFieldNames[i]] = keysList[i];
                        db.set ("conf_" + this.keyFieldNames[i], keysList[i]);
                        if (this.keyFieldNames[0] == Configuration.RECORD_LEVEL) {
                            this.isKeyed = false;
                        } else {
                            this.isKeyed = true;
                        }
                    }
                    db.set (Configuration.KEY_MODE, this.isKeyed);
                    db.set (Configuration.KEY_NAMES, this.keyFieldNames);

                }
            } catch (Exception ex) {
                throw new StoreInitException ("Store init error: " + ex.Message);
            }
        }

        public Configuration (IStoreEngine db, Key[] keysList) {
            this.keyFieldNames = new String[keysList.Length];
            for (int i = 0; i < keysList.Length; i++) {
                this.keyFieldNames[i] = keysList[i].keyFieldName;
                keys[keyFieldNames[i]] = keysList[i];
                db.set ("conf_" + this.keyFieldNames[i], keysList[i]);
                if (this.keyFieldNames[0] == Configuration.RECORD_LEVEL) {
                    this.isKeyed = false;
                } else {
                    this.isKeyed = true;
                }
            }
            db.set (Configuration.KEY_MODE, this.isKeyed);
            db.set (Configuration.KEY_NAMES, this.keyFieldNames);
        }
    }
}