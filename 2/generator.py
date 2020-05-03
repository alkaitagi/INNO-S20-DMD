import itertools


queries = [
    {
        "rental": ["staff_id", "rental_id", "customer_id", "last_update"],
        "payment": ["rental_id", "payment_date"],
        "customer": ["customer_id"]
    },
    {
        "film": ["title", "release_year", "rental_rate"]
    }
]


for q, query in enumerate(queries):
    indexes = {}

    for table, columns in query.items():
        for i in range(1, len(columns) + 1):
            for combination in itertools.combinations(columns, i):
                name = f"query_{q}_{len(indexes)}"
                indexes[name] = \
                    f"CREATE INDEX {name} ON {table} ({', '.join(combination)});"

    for name, index in indexes.items():
        print(index)

    print()
    [print(f"DROP INDEX {name};") for name in indexes]
    print("----------")
