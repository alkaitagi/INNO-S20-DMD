import itertools


queries = [
    {
        "rental": ["staff_id", "rental_id", "customer_id", "last_update"],
        "payment": ["rental_id", "payment_date"],
        "customer": ["customer_id"]
    },
    {
        "film": ["title", "release_year", "rental_rate"]
    },
    {
        "film": ["title", "release_year", "film_id", "rating"],
        "customer": ["customer_id", "first_name"],
        "rental": ["rental_id", "inventory_id", "customer_id"],
        "inventory": ["inventory_id", "film_id"],
        "payment": ["rental_id"],
        "film_actor": ["film_id", "actor_id"],
        "actor": ["actor_id", "first_name"]
    }
]


for q, query in enumerate(queries):
    indexes = {}

    for table, columns in query.items():
        for i in range(1, len(columns) + 1):
            for combination in itertools.combinations(columns, i):
                name = f"query_{q}_{len(indexes) // 2}"
                indexes[name + "_b"] = \
                    f"CREATE INDEX {name}_b ON {table} ({', '.join(combination)});"
                indexes[name + "_h"] = \
                    f"CREATE INDEX {name}_h ON {table} USING hash ({', '.join(combination)});"

    for name, index in indexes.items():
        print(index)

    print()
    [print(f"DROP INDEX {name};") for name in indexes]
    print("----------")
