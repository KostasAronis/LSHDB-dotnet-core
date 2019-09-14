using System;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using System.Xml.Serialization;
using LevelDB;
using LSHDBLib.Base;

namespace LSHDBLib.StoreEngine
{
    public class LevelDBEngineFactory : IStoreEngineFactory
    {
        public IStoreEngine createInstance(string folder, string storeName, string entity, bool massInsertMode)
        {
            if(!String.IsNullOrEmpty(entity) && !String.IsNullOrWhiteSpace(entity)){
                storeName+="-"+entity;
            }
            return new LevelDBEngine(folder, storeName, massInsertMode);
        }
    }
    public class LevelDBEngine : IStoreEngine
    {
        public DB _db;
        private WriteBatch batch;

        public LevelDBEngine(string folder, string storeName,  bool massInsertMode)
        {
            string pathToDb = Path.Combine(folder,storeName);
            var options = new Options { CreateIfMissing = true };
            options.CompressionLevel = CompressionLevel.NoCompression;
            _db = new DB(options, pathToDb);
            options.Cache = new Cache(600 *  1048576);
            if (massInsertMode){
                throw new NotImplementedException("batch insert not implemented for leveldb");
            }
        }

        public void close()
        {
            _db.Close();
        }

        public bool contains(string key)
        {
            try
            {
                return _db.Get(key)!=null;
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine(ex.Message);
                return false;
            }
        }

        public long count()
        {
            return 0;
        }

        public Iterable createIterator()
        {
            return new LevelDBIterator(this);
        }

        public object get(string key)
        {
            return Converters.StringToObject(_db.Get(key));
        }

        public void set(string key, object data)
        {
            _db.Put(key,Converters.ObjectToString(data));
        }
    }
    public class LevelDBIterator : Iterable {
        Iterator iterator;

        public LevelDBIterator(LevelDBEngine db){
            ReadOptions ro = new ReadOptions(){FillCache=true};
            iterator = db._db.CreateIterator();
        }

        public void close()
        {
            iterator.Dispose();
        }

        public string getKey()
        {
            iterator.Next();
            string key = Encoding.Default.GetString(iterator.Key());
            iterator.Prev();
            return key;
        }

        public object getValue()
        {
            return Converters.ByteArrayToObject(iterator.Value());
        }

        public bool hasNext()
        {
            iterator.Next();
            bool valid = iterator.IsValid();
            iterator.Prev();
            return valid;
        }
        public void next()
        {
            iterator.Next();
        }

        public void seek(string partialKey)
        {
            iterator.Seek(partialKey);
        }
    }
    public static class Converters{
        public static string ObjectToXMLString(Object obj){
            XmlSerializer xmlSerializer = new XmlSerializer(obj.GetType());
            using(StringWriter textWriter = new StringWriter())
            {
                xmlSerializer.Serialize(textWriter, obj);
                return textWriter.ToString();
            }
        }
        public static Object XMLStringToObject(string str){
            XmlSerializer xmlSerializer = new XmlSerializer(new Object().GetType());
            using(TextReader reader = new StringReader(str))
            {
                return xmlSerializer.Deserialize(reader);
            }
        }
        public static string ObjectToString(Object obj){
            return Convert.ToBase64String(ObjectToByteArray(obj));
        }
        public static Object StringToObject(string str){
            return ByteArrayToObject(Convert.FromBase64String(str));
        }
        public static byte[] ObjectToByteArray(Object obj)
        {
            BinaryFormatter bf = new BinaryFormatter();
            using (var ms = new MemoryStream())
            {
                bf.Serialize(ms, obj);
                return ms.ToArray();
            }
        }
        public static Object ByteArrayToObject(byte[] arrBytes)
        {
            using (var memStream = new MemoryStream())
            {
                var binForm = new BinaryFormatter();
                memStream.Write(arrBytes, 0, arrBytes.Length);
                memStream.Seek(0, SeekOrigin.Begin);
                var obj = binForm.Deserialize(memStream);
                return obj;
            }
        }
    }
}