import duckdb
con = duckdb.connect("taxi.duckdb")
# Drop all dbt-created objects
for schema in ['marts', 'intermediate', 'staging']:
    tables = con.sql(f"SELECT table_name, table_type FROM information_schema.tables WHERE table_schema='{schema}'").fetchall()
    for name, ttype in tables:
        if ttype == 'VIEW':
            con.sql(f'DROP VIEW IF EXISTS {schema}."{name}"')
        else:
            con.sql(f'DROP TABLE IF EXISTS {schema}."{name}"')
        print(f"Dropped {ttype} {schema}.{name}")
print("Done - all dbt objects dropped")
con.close()
