from pathlib import Path
from pymongo import MongoClient
import csv


client = MongoClient("localhost", 27017)

cursor = client["dvdrental"]["film"].aggregate(
    [
        {
            u"$project": {
                u"_id": 0,
                u"film": u"$$ROOT"
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
            u"$lookup": {
                u"localField": u"film_category.category_id",
                u"from": u"category",
                u"foreignField": u"category_id",
                u"as": u"category"
            }
        },
        {
            u"$unwind": {
                u"path": u"$category",
                u"preserveNullAndEmptyArrays": False
            }
        },
        {
            u"$project": {
                u"id": u"$film.film_id",
                u"title": u"$film.title",
                u"category": u"$category.name",
                u"_id": 0
            }
        },
        {
            u"$sort": {
                "id": 1
            }
        }
    ],
    allowDiskUse=True
)
films = []
for film in cursor:
    films.append(film)

cursor = client["dvdrental"]["customer"].aggregate(
    [
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
            u"$group": {
                u"_id": {
                    u"film\u1390film_id": u"$film.film_id",
                    u"customer\u1390customer_id": u"$customer.customer_id"
                }
            }
        },
        {
            u"$project": {
                u"customer_id": u"$_id.customer\u1390customer_id",
                u"film_id": u"$_id.film\u1390film_id",
                u"_id": 0
            }
        }
    ],
    allowDiskUse=True
)
rentals = []
for rental in cursor:
    rentals.append(rental)


client.close()
Path("1/results").mkdir(exist_ok=True)


with open("1/results/3.csv", "w") as file:
    sheet = csv.writer(file, lineterminator='\n')
    sheet.writerow(["film", "category", "times rented"])

    for film in films:
        sheet.writerow([
            film["title"],
            film["category"],
            sum(r["film_id"] == film["id"] for r in rentals)
        ])
