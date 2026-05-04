import argparse
import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm

dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]

def get_df_from_csv_file(csv_file):
    df = pd.read_csv(
        csv_file,
        chunksize=10000,
        dtype=dtype,
        parse_dates=parse_dates
    )
    return df

def create_table_from_df(df: pd.DataFrame, engine, table_name: str) -> None:
    pd.io.sql.get_schema(df, name=f'{table_name}', con=engine)
    df.head(0).to_sql(name=f'{table_name}', con=engine, if_exists='replace')

def insert_df_to_db(df: pd.DataFrame, engine, table_name: str) -> None:
    df.to_sql(name=f'{table_name}', con=engine, if_exists='append') 
    
def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Import data from a CSV 'yellow_tripdata' file into a PostgreSQL database."
        )
    )
    parser.add_argument("--period", default="2021-01", help="Période des données à traiter (ex: 2021-01)")
    parser.add_argument("--db_name", default="ny_taxi", help="Nom de la base de données (ex: ny_taxi)")
    parser.add_argument("--pg_user", default="root", help="PostgreSQL user")
    parser.add_argument("--pg_pass", default="root", help="PostgreSQL password")
    parser.add_argument("--pg_host", default="localhost", help="PostgreSQL host")
    parser.add_argument("--pg_port", default="5432", help="PostgreSQL port")
    parser.add_argument("--pg_db", default="ny_taxi", help="PostgreSQL database")

    return parser.parse_args()

if __name__ == "__main__":

    args = parse_arguments()

    prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/yellow'
    csv_file = prefix + f'yellow_tripdata_{args.period}.csv.gz'
    df_iter  = get_df_from_csv_file(csv_file)

    engine = create_engine(f'postgresql+psycopg://{args.pg_user}:{args.pg_pass}@{args.pg_host}:{args.pg_port}/{args.pg_db}')
    table_name = f'yellow_taxi_{args.period.replace("-", "_")}'
    first = True
    for df_chunk in tqdm(df_iter):
        if first:
            create_table_from_df(df_chunk, engine, table_name)
            first = False
        insert_df_to_db(df_chunk, engine, table_name)