using System;
using System.Collections.Generic;
using LSHDBLib.Base;

namespace LSHDBLib.Result
{
    [Serializable]
    public class Record
    {
        public Dictionary<String, Object> record = new Dictionary<String, Object>();
        public Dictionary<String, int> notIndexedFields = new Dictionary<String, int>();

        public static String PRIVATE_STRUCTURE = "PRIVATE";
        public static String REMOTE_RECORD = "remote";
        public bool remote {get;set;} = false;

        public Dictionary<String, Object> toJsonObject() {
            foreach (String key in record.Keys)
            {
                if(key.EndsWith(Key.TOKENS)){
                    record.Remove(key);
                }
            }
            record[REMOTE_RECORD]=remote;
            return record;
        }

        public void set(String fieldName, Object fieldValue) {
            record[fieldName]=fieldValue;
        }

        public void setNotIndexedField(String fieldName) {
            notIndexedFields[fieldName] = 1;
        }
        public bool isNotIndexedField(String fieldName) {
            return notIndexedFields.ContainsKey(fieldName);
        }
        public Object get(String fieldName) {
            return record[fieldName];
        }

        public String getIdFieldName() {
            return "Id";
        }

        public String getId() {
            return (String) record["Id"];
        }
        public void setId(String id) {
            record["Id"] = id;
        }

        public List<String> getFieldNames() {
            List<String> arr = new List<String>();
            foreach (String key in record.Keys)
            {
                if ( !key.Equals(this.getIdFieldName()) && (!key.EndsWith(Key.TOKENS))){
                    arr.Add(key);
                }
            }
            return arr;
        }
    }
}