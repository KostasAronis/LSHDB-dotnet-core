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
            return new JSONFileEngine(Path.Combine(folder,storeName+"_"+entity));
        }
    }

    public class JSONFileEngine : IStoreEngine {
        string _filepath;
        Dictionary<string, Object> _dict;
        public JSONFileEngine (string filePath) {
            var dir = Path.GetDirectoryName(filePath);
            if (!Path.HasExtension(filePath) || Path.GetExtension(filePath) != "json")
            {
                filePath += ".json";
            }
            _filepath = filePath;

            if (File.Exists(filePath))
            {
                var text = File.ReadAllText(filePath);
                _dict = JsonConvert.DeserializeObject<Dictionary<string, Object>> (text);
            }
            else
            {
                _dict = new Dictionary<string, object>();
            }
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
        private int index = 0;
        private int maxIndex;
        Dictionary<string,object>.KeyCollection.Enumerator keys;
        private List<object> values;
        public JSONFileIterable (Dictionary<string, object> dict) {
            _dict = dict;
            maxIndex = dict.Count;
            keys = dict.Keys.GetEnumerator();
        }
        public void close () {
            return;
        }
        public string getKey () {
            return keys.Current;
        }
        public object getValue () {
            return _dict[keys.Current];
        }
        public bool hasNext () {
            return index < maxIndex;
        }
        public void next () {
            index++;
            keys.MoveNext();
        }
        public void seek (string partialKey) {
            throw new NotImplementedException ();
        }
    }
}