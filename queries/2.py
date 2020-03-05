from pymongo import MongoClient

client = MongoClient("localhost", 27017)
database = client["dvdrental"]
collection = database["film_actor"]

pipeline = [
    {
        u"$project": {
            u"_id": 0,
            u"F1": u"$$ROOT"
        }
    },
    {
        u"$lookup": {
            u"localField": u"F1.film_id",
            u"from": u"film_actor",
            u"foreignField": u"film_id",
            u"as": u"F2"
        }
    },
    {
        u"$unwind": {
            u"path": u"$F2",
            u"preserveNullAndEmptyArrays": False
        }
    },
    {
        u"$group": {
            u"_id": {
                u"actor1": u"$F1.actor_id",
                u"actor2": u"$F2.actor_id"
            },
            u"films": {
                u"$sum": 1
            }
        }
    },
    {
        u"$project": {
            u"_id": 0,
            u"actor1": u"$_id.actor1",
            u"actor2": u"$_id.actor2",
            u"films": u"$films"
        }
    }
]

cursor = collection.aggregate(
    pipeline,
    allowDiskUse=True
)
try:
    for doc in cursor:
        print(doc)
finally:
    client.close()
