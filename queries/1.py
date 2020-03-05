from pymongo import MongoClient
from datetime import datetime

client = MongoClient("localhost", 27017)
database = client["dvdrental"]
collection = database["customer"]

pipeline = [
    {
        u"$project": {
            u"_id": 0,
            u"customer": u"$$ROOT"
        }
    },
    {
        u"$lookup": {
            u"localField": u"customer.customer_id",
            u"from": u"rental",
            u"foreignField": u"customer_id",
            u"as": u"rental"
        }
    },
    {
        u"$unwind": {
            u"path": u"$rental",
            u"preserveNullAndEmptyArrays": False
        }
    },
    {
        u"$lookup": {
            u"localField": u"rental.inventory_id",
            u"from": u"inventory",
            u"foreignField": u"inventory_id",
            u"as": u"inventory"
        }
    },
    {
        u"$unwind": {
            u"path": u"$inventory",
            u"preserveNullAndEmptyArrays": False
        }
    },
    {
        u"$lookup": {
            u"localField": u"inventory.film_id",
            u"from": u"film",
            u"foreignField": u"film_id",
            u"as": u"film"
        }
    },
    {
        u"$unwind": {
            u"path": u"$film",
            u"preserveNullAndEmptyArrays": False
        }
    },
    {
        u"$lookup": {
            u"localField": u"film.film_id",
            u"from": u"film_category",
            u"foreignField": u"film_id",
            u"as": u"film_category"
        }
    },
    {
        u"$unwind": {
            u"path": u"$film_category",
            u"preserveNullAndEmptyArrays": False
        }
    },
    {
        u"$match": {
            u"rental.rental_date": {
                u"$gte": datetime(2006, 1, 1)
            }
        }
    },
    {
        u"$group": {
            u"_id": {
                u"customer\u1390customer_id": u"$customer.customer_id",
                u"customer\u1390last_name": u"$customer.last_name",
                u"customer\u1390first_name": u"$customer.first_name"
            },
            u"COUNT(film_category\u1390category_id)": {
                u"$sum": 1
            }
        }
    },
    {
        u"$project": {
            u"customer.customer_id": u"$_id.customer\u1390customer_id",
            u"customer.first_name": u"$_id.customer\u1390first_name",
            u"customer.last_name": u"$_id.customer\u1390last_name",
            u"COUNT(film_category.category_id)": u"$COUNT(film_category\u1390category_id)",
            u"_id": 0
        }
    },
    {
        u"$match": {
            u"COUNT(film_category.category_id)": {
                u"$gte": 2
            }
        }
    },
    {
        u"$project": {
            u"_id": 0,
            u"customer.customer_id": u"$customer.customer_id",
            u"customer.first_name": u"$customer.first_name",
            u"customer.last_name": u"$customer.last_name"
        }
    }
]

cursor = collection.aggregate(
    pipeline,
    allowDiskUse=True
)
print("Rented more than 2 categories of movies:")
try:
    with open('results/1.txt', 'w') as file:
        for doc in cursor:
            file.write(doc["customer"]["first_name"] + " " + doc["customer"]["last_name"] + "\n")
finally:
    client.close()
