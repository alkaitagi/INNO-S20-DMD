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


search(target, 0)
for visited, distance in distance.items():
    print(f"{visited} - {actors[visited]}: {distance}")

# with open(curdir + "\\5.csv", "w") as file:
#     sheet = csv.writer(file, lineterminator='\n')

#     max_id = max(r["actor1"] for r in records)
#     id1, id2 = 0, max_id + 1
#     row = ["ID"] + list(actors[id] for id in range(1, max_id + 1))
#     for r in records:
#         if r["actor1"] != id1:
#             if id2 <= max_id:
#                 row += [0] * (max_id - id2 + 1)
#             sheet.writerow(row)

#             id1 += 1
#             id2 = 1
#             row = [actors[id1]]

#         if r["actor2"] != id2:
#             row += [0] * (r["actor2"] - id2)
#             id2 = r["actor2"]

#         row.append(r["films"])
#         id2 += 1

#     if len(row) > 1:
#         if id2 <= max_id:
#             row += [0] * (max_id - id2 + 1)
#         sheet.writerow(row)
