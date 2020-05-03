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
            for permutation in itertools.permutations(columns, i):
                l = len(indexes)
                name = f"query_{q}_"

                indexes[name + str(l)] = \
                    f"CREATE INDEX {name} ON {table} ({', '.join(permutation)});"
                    
                if len(permutation) == 1:
                    indexes[name + str(l + 1)] = \
                        f"CREATE INDEX {name} ON {table} USING hash ({', '.join(permutation)});"

    for name, index in indexes.items():
        print(index)

    print()
    [print(f"DROP INDEX {name};") for name in indexes]
    print("----------")
