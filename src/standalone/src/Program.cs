using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using LSHDBLib;
using LSHDBLib.Base;
using LSHDBLib.Embeddables;
using LSHDBLib.Hamming;
using LSHDBLib.Result;
using LSHDBLib.StoreEngine;

namespace standalone
{
    class Program
    {
        static void Main(string[] args)
        {
            try{
                String folder = "./data/db";
                String dbName = "qq.json";
                Key key1 = new HammingKey("recordLevel", 32, .1, 106, false, true, new BloomFilter(1000,10,2));
                JSONFileEngine engine = new JSONFileEngine("./data/asdf.json");
                HammingConfiguration hc = new HammingConfiguration(folder, dbName, engine, new Key[]{key1},true);
                JSONFileEngineFactory engineFactory = new JSONFileEngineFactory();
                HammingLSHStore lsh = new HammingLSHStore(folder, dbName, engineFactory, hc, true);
                String file = "./data/test_voters.txt";
                List<string> lines = File.ReadAllLines(file).ToList();
                Random r = new Random();
                for (int i = 0; i < lines.Count; i++) {
                    string line = lines[i];
                    List<string> tokens=line.Split(',').ToList();
                    string id = tokens[0];
                    string lastName = tokens[1];
                    string firstName = tokens[2];
                    string address = tokens[3];
                    string town = tokens.Count == 5 ? tokens[4] : "";
                    Record rec = new Record();
                    rec.setId(id);
                    rec.set("Last Name", lastName);
                    rec.set("First Name", firstName);
                    rec.set("Address", address );
                    rec.set("Town", town);
                    lsh.insert(rec);
                }
                lsh.close();
                hc.close();
            } catch (Exception ex) {//Catch exception if any
                Console.Error.WriteLine(ex.Message);
            }
        }
    }
}
