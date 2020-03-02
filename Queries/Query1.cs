using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Threading.Tasks;

namespace INNO_S20_DMD_1
{
    public class Query1
    {
        public static void Start()
        {
            IMongoClient client = new MongoClient("mongodb://localhost:27017");
            IMongoDatabase database = client.GetDatabase("dvdrental");
            IMongoCollection<BsonDocument> collection = database.GetCollection<BsonDocument>("customer");

            var options = new AggregateOptions()
            {
                AllowDiskUse = true
            };

            PipelineDefinition<BsonDocument, BsonDocument> pipeline = new BsonDocument[]
            {
                new BsonDocument("$project", new BsonDocument()
                        .Add("_id", 0)
                        .Add("customer", "$$ROOT")),
                new BsonDocument("$lookup", new BsonDocument()
                        .Add("localField", "customer.customer_id")
                        .Add("from", "rental")
                        .Add("foreignField", "customer_id")
                        .Add("as", "rental")),
                new BsonDocument("$unwind", new BsonDocument()
                        .Add("path", "$rental")
                        .Add("preserveNullAndEmptyArrays", new BsonBoolean(false))),
                new BsonDocument("$lookup", new BsonDocument()
                        .Add("localField", "rental.inventory_id")
                        .Add("from", "inventory")
                        .Add("foreignField", "inventory_id")
                        .Add("as", "inventory")),
                new BsonDocument("$unwind", new BsonDocument()
                        .Add("path", "$inventory")
                        .Add("preserveNullAndEmptyArrays", new BsonBoolean(false))),
                new BsonDocument("$lookup", new BsonDocument()
                        .Add("localField", "inventory.film_id")
                        .Add("from", "film")
                        .Add("foreignField", "film_id")
                        .Add("as", "film")),
                new BsonDocument("$unwind", new BsonDocument()
                        .Add("path", "$film")
                        .Add("preserveNullAndEmptyArrays", new BsonBoolean(false))),
                new BsonDocument("$lookup", new BsonDocument()
                        .Add("localField", "film.film_id")
                        .Add("from", "film_category")
                        .Add("foreignField", "film_id")
                        .Add("as", "film_category")),
                new BsonDocument("$unwind", new BsonDocument()
                        .Add("path", "$film_category")
                        .Add("preserveNullAndEmptyArrays", new BsonBoolean(false))),
                new BsonDocument("$match", new BsonDocument()
                        .Add("$and", new BsonArray()
                                .Add(new BsonDocument()
                                        .Add("rental.rental_date", new BsonDocument()
                                                .Add("$gte", BsonDateTime.Create("2006-01-01"))
                                        )
                                )
                                .Add(new BsonDocument()
                                        .Add("rental.rental_date", new BsonDocument()
                                                .Add("$lt", BsonDateTime.Create("2007-01-01"))
                                        )
                                )
                        )),
                new BsonDocument("$project", new BsonDocument()
                        .Add("customer.first_name", "$customer.first_name")
                        .Add("_id", 0))
            };

            using var cursor = collection.Aggregate(pipeline, options);
            while (cursor.MoveNext())
            {
                var batch = cursor.Current;
                foreach (BsonDocument document in batch)
                {
                    Console.WriteLine(document.ToJson());
                }
            }
        }
    }
}
