using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using CommandLine;
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
        public class Options
        {
            [Option('d', "database", Required = true, HelpText = "Database to be processed.")]
            public string Database { get; set; }

            [Option('c', "config", Required = false, HelpText = "Database in which to store configuration.")]
            public string ConfigDatabase { get; set; }
            
            [Option('p', "path", Required = false, HelpText = "Path (relative to executable) in which to store databases. (default: ./data/db)")]
            public string Path { get; set; }
            
            [Option('i', "input", Required = false, HelpText = "Path (relative to executable) to the input file. (default: ./data/test_voters.txt)")]
            public string Input { get; set; }

            [Option('r',"readOnly", Default=false, HelpText = "Only read the data in the db without writing. (default: false)")]
            public bool ReadOnly { get; set; }
        }
        static String DEFAULT_FOLDER = Path.Combine("data", "db");
        static String DEFAULT_CONFIGURATION_DB = "conf";
        static String DEFAULT_DB_NAME = "database";
        static String DEFAULT_INPUT_FILE = Path.Combine("data", "test_voters.txt");

        static void Main(string[] args)
        {
            CommandLine.Parser.Default.ParseArguments<Options>(args)
                .WithParsed<Options>(opts => {
                    if(opts.ReadOnly){
                        Console.WriteLine("Reading");
                        var path = String.IsNullOrEmpty(opts.Path)?DEFAULT_FOLDER:opts.Path;
                        var db = opts.Database;
                        Read(path,db);
                    } else {
                        Write(opts.Path,opts.ConfigDatabase,opts.Database,opts.Input);
                    }
                })
                .WithNotParsed<Options>((errs) => {
                    Console.Error.WriteLine(string.Join("\n",errs.Select(e=>e.ToString())));
                });
        }
        static void Read(String path, String dbName){
            JSONFileEngine db = new JSONFileEngine(Path.Combine(path, dbName + "_records"));
            var iter = db.createIterator();
            while(iter.hasNext()){
                iter.next();
                Debug.WriteLine(iter.getKey() + ": " + iter.getValue());
                Console.WriteLine(iter.getKey()+": "+iter.getValue());
            }
        }
        static void Write(String folder="", String confDB = "", String dbName="", String file=""){
            try{
                folder = !String.IsNullOrEmpty(folder) ? folder : DEFAULT_FOLDER;
                confDB = !String.IsNullOrEmpty(confDB) ? confDB : DEFAULT_CONFIGURATION_DB;
                dbName = !String.IsNullOrEmpty(dbName) ? dbName : DEFAULT_DB_NAME;
                file = !String.IsNullOrEmpty(file) ? file :DEFAULT_INPUT_FILE;
                if (!Directory.Exists(folder))
                {
                    Directory.CreateDirectory(folder);
                }
                Key key1 = new HammingKey("recordLevel", 32, .1, 106, false, true, new BloomFilter(1000,10,2));
                //LevelDBEngine engine = new LevelDBEngine(folder, confDB, false);
                JSONFileEngine engine = new JSONFileEngine(Path.Combine(folder, confDB));
                HammingConfiguration hc = new HammingConfiguration(folder, dbName, engine, new Key[]{key1},false);
                //LevelDBEngineFactory engineFactory = new LevelDBEngineFactory();
                JSONFileEngineFactory engineFactory = new JSONFileEngineFactory();
                HammingLSHStore lsh = new HammingLSHStore(folder, dbName, engineFactory, hc, false);
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
