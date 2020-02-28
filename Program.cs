using System.IO;
using System.Linq;
using System.Collections.Generic;
using Npgsql;
using MongoDB.Bson;
using MongoDB.Driver;

namespace INNO_S20_DMD_1
{
    class Program
    {
        static IEnumerable<(NpgsqlDataReader table, string name)> GetPSQLTables()
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

        static void Main()
        {
            var mongoHost = new MongoClient("mongodb://localhost:27017");
            mongoHost.DropDatabase("dvdrental");
            var mongo = mongoHost.GetDatabase("dvdrental");

            foreach (var (table, name) in GetPSQLTables())
            {
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
        }
    }
}
