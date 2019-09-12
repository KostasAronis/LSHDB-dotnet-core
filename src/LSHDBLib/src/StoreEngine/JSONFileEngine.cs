using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using LSHDBLib.Base;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace LSHDBLib.StoreEngine {

    public class JSONFileEngineFactory : IStoreEngineFactory
    {
        public IStoreEngine createInstance(string folder, string storeName, string entity, bool massInsertMode)
        {
            return new JSONFileEngine(Path.Combine(folder,storeName));
        }
    }

    public class JSONFileEngine : IStoreEngine {
        string _filepath;
        Dictionary<string, Object> _dict;
        public JSONFileEngine (string filePath) {
            _filepath = filePath;
            var text = File.ReadAllText(filePath);
            _dict = JsonConvert.DeserializeObject<Dictionary<string, Object>> (text);
        }
        public void close () {
            File.WriteAllText(_filepath, JsonConvert.SerializeObject (_dict));
        }
        public bool contains (string key) {
            return _dict.ContainsKey(key);
        }
        public long count () {
            return _dict.Count;
        }
        public Iterable createIterator () {
            return new JSONFileIterable(_dict);
        }
        public Object get (string key) {
            Object val;
            if (_dict.TryGetValue(key, out val)) {
                return (val as JArray).ToObject<Object>();
            }
            return null;
        }
        public void set (string key, Object data) {
            _dict[key] = data;
        }
    }
    internal class JSONFileIterable : Iterable {
        private Dictionary<string, object> _dict;

        public JSONFileIterable (Dictionary<string, object> dict) {
            _dict = dict;
        }
        public void close () {
            throw new NotImplementedException ();
        }
        public string getKey () {
            throw new NotImplementedException ();
        }
        public object getValue () {
            throw new NotImplementedException ();
        }
        public bool hasNext () {
            throw new NotImplementedException ();
        }
        public void next () {
            throw new NotImplementedException ();
        }
        public void seek (string partialKey) {
            throw new NotImplementedException ();
        }
    }
}