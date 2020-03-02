using System;
using System.IO;
using System.Linq;
using System.Collections.Generic;
using Npgsql;
using MongoDB.Bson;
using MongoDB.Driver;

namespace INNO_S20_DMD_1
{
    class Migration
    {
        private static void PopulatePSQL()
        {
            Console.WriteLine("Populating Postgres...");
            System.Diagnostics.Process.Start
            (
                "CMD.exe",
                "psql -U postgres -h localhost -p 5432 -d dvdrental -f"
                + "assignment\\postgree\\restore.sql"
            );
        }

        private static IEnumerable<(NpgsqlDataReader table, string name)> GetPSQLTables()
        {
            static IEnumerable<string> GetTableNames(NpgsqlConnection psql)
            {
                using var query = new NpgsqlCommand
                (
                    @"SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'public' and table_type = 'BASE TABLE';",
                    psql
                )
                .ExecuteReader();

                var names = new List<string>();
                while (query.Read())
                    names.Add(query[0].ToString());

                query.Close();
                return names;
            }

            using var psql = new NpgsqlConnection
            (
                "Server=127.0.0.1;"
                + "User Id=postgres;"
                + $"Password={File.ReadAllText("password.txt")};"
                + "Database=dvdrental;"
            );
            psql.Open();

            foreach (var name in GetTableNames(psql))
                yield return
                (
                    new NpgsqlCommand($"SELECT * FROM {name}", psql).ExecuteReader(),
                    name
                );

            psql.Close();
        }

        private static IMongoDatabase ConnectToMGDB()
        {
            Console.WriteLine("Dropping MGDB...");
            var mongoHost = new MongoClient("mongodb://localhost:27017");
            mongoHost.DropDatabase("dvdrental");
            return mongoHost.GetDatabase("dvdrental");
        }

        public static void Start()
        {
            Console.WriteLine("Beginning migration...");
            PopulatePSQL();
            var mongo = ConnectToMGDB();

            foreach (var (table, name) in GetPSQLTables())
            {
                Console.WriteLine($"Moving {name}...");
                var columns = table.GetColumnSchema().Select(c => c.ColumnName).ToArray();
                var rows = new List<BsonDocument>();

                while (table.Read())
                {
                    var item = new BsonDocument();
                    for (int i = 0; i < table.FieldCount; i++)
                    {
                        BsonValue value;
                        try { value = BsonValue.Create(table[i]); }
                        catch { value = BsonValue.Create(table[i].ToString()); }
                        item.Add(columns[i], value);
                    }
                    rows.Add(item);
                }

                table.Close();
                mongo.GetCollection<BsonDocument>(name).InsertMany(rows);
            }
            Console.WriteLine("Migration has finished\n");
        }
    }
}
