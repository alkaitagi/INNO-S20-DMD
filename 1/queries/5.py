from pathlib import Path
from pymongo import MongoClient
import csv
import os
import math


client = MongoClient("localhost", 27017)


cursor = client["dvdrental"]["actor"].find({})
actors = {}
for doc in cursor:
    actors[doc["actor_id"]] = f"{doc['first_name']} {doc['last_name']}"


target = int(input("Target actor id: "))
if target not in actors:
    print("No such actor")
    exit()


cursor = client["dvdrental"]["film_actor"].aggregate(
    [
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
                }
            }
        },
        {
            u"$project": {
                u"_id": 0,
                u"actor1": u"$_id.actor1",
                u"actor2": u"$_id.actor2"
            }
        },
        {
            u"$sort": {
                "actor1": 1,
                "actor2": 1
            }
        }
    ],
    allowDiskUse=True
)
records = []
for doc in cursor:
    records.append(doc)


client.close()
curdir = os.path.dirname(os.path.dirname(
    os.path.abspath(__file__))) + "\\results"
Path(curdir).mkdir(exist_ok=True)


distance = {}


def search(current, steps):
    if distance.get(current, math.inf) > steps:
        distance[current] = steps
        for next in [r["actor2"] for r in records if r["actor1"] == current]:
            search(next, steps + 1)


with open(curdir + "\\5.csv", "w") as file:
    sheet = csv.writer(file, lineterminator='\n')
    search(target, 0)
    distance.pop(target, None)

    sheet.writerow([f"Distance from {actors[target]} to"])
    for visited, distance in distance.items():
        sheet.writerow([actors[visited], distance])
