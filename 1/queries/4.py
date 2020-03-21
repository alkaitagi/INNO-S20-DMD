from pathlib import Path
from pymongo import MongoClient
import os
import csv


client = MongoClient("localhost", 27017)


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
                    u"film\u1390title": u"$film.title",
                    u"customer\u1390customer_id": u"$customer.customer_id",
                    u"customer\u1390last_name": u"$customer.last_name",
                    u"customer\u1390first_name": u"$customer.first_name"
                }
            }
        },
        {
            u"$project": {
                u"customer_id": u"$_id.customer\u1390customer_id",
                u"first_name": u"$_id.customer\u1390first_name",
                u"last_name": u"$_id.customer\u1390last_name",
                u"film_id": u"$_id.film\u1390film_id",
                u"film": u"$_id.film\u1390title",
                u"_id": 0
            }
        },
        {
            u"$sort": {
                "customer_id": 1
            }
        }
    ],
    allowDiskUse=True
)
rentals = []
for rental in cursor:
    rentals.append(rental)


client.close()
curdir = os.path.dirname(os.path.dirname(
    os.path.abspath(__file__))) + "\\results"
Path(curdir).mkdir(exist_ok=True)


target = int(input("Target customer's id: "))


customers = {}
for rental in rentals:
    id = rental["customer_id"]
    if id not in customers:
        customers[id] = {
            "name": rental["first_name"] + " " + rental["last_name"],
            "films": {}
        }

    customers[id]["films"][rental["film_id"]] = rental["film"]


target_films = customers[target]["films"].keys()
result_films = {}
CD = 1 / len(customers)
KD = 1 / len(target_films)
for _, customer in customers.items():
    K = KD * sum(f in target_films for f in customer["films"])
    for film in [f for f in customer["films"] if f not in target_films]:
        if film not in result_films:
            result_films[film] = {
                "title": customer["films"][film],
                "C": 0
            }
        result_films[film]["C"] += CD * K


with open(curdir + "\\4.csv", "w") as file:
    sheet = csv.writer(file, lineterminator='\n')
    sheet.writerow(["Recommendations for", customers[target]['name']])

    for id in sorted(result_films, key=lambda f: result_films[f]["C"], reverse=True):
        film = result_films[id]
        sheet.writerow([film["title"], film["C"]])
