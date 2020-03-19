import psycopg2
import types
import decimal
from datetime import date
from datetime import datetime
from pymongo import MongoClient
from bson.decimal128 import Decimal128
import os
import subprocess


curdir = os.path.dirname(os.path.abspath(__file__))


print("Populating PSQL...")
print(curdir + "\\postgres\\restore.sql")
os.system(f"psql -U postgres -h localhost -p 5432 -d dvdrental -f " +
          curdir + "\\postgres\\restore.sql")


psql = psycopg2.connect(user="postgres",
                        password=input("Password for user postgres: "),
                        host="127.0.0.1",
                        port="5432",
                        database="dvdrental")
cursor = psql.cursor()
mgdb = MongoClient("localhost", 27017)
mgdb.drop_database("dvdrental")


print("\nPopulating MGDB...")
cursor.execute(
    "SELECT table_name FROM information_schema.tables WHERE table_schema='public' and table_type='BASE TABLE'")
for table in [t[0] for t in cursor.fetchall()]:
    print(f"Moving {table}...")

    cursor.execute(f"SELECT * FROM {table}")
    columns = [d[0] for d in cursor.description]
    rows = []

    for record in cursor.fetchall():
        row = {}
        i = 0

        for field in record:
            if isinstance(field, decimal.Decimal):
                field = Decimal128(str(field))
            elif isinstance(field, date):
                field = datetime(field.year, field.month, field.day)
            elif isinstance(field, memoryview):
                field = str(field)

            row[columns[i]] = field
            i += 1

        rows.append(row)

    mgdb["dvdrental"][table].insert_many(rows)
