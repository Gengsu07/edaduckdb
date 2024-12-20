import duckdb

connection_string = (
    "host=10.20.254.226"
    " port=5432"
    " user=gengsu07"
    " dbname=ppmpkm"
    " password=Gengsu!sh3r3"
    " sslmode=disable"
)


def setup_postgres_connection(connection_string):
    con = duckdb.connect()
    con.install_extension("postgres")
    con.load_extension("postgres")

    # Just attach the postgres database
    con.execute(f"ATTACH '{connection_string}' as db (TYPE POSTGRES, SCHEMA 'public')")

    return con


# Usage example:
con = setup_postgres_connection(connection_string)

# Now you can query directly
result = con.sql("SELECT * FROM db.public.ppmpkm LIMIT 10;").fetchall()
print(result)
