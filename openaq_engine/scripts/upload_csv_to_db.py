import os

import pandas as pd
from sqlalchemy import create_engine, text


def create_table_from_csv(engine, table_name, csv_file):
    """
    Create a table in the database based on the CSV file structure.
    """
    df = pd.read_csv(
        csv_file, nrows=1
    )  # Read only the header row to infer the schema
    columns = df.columns

    # Quote the table name to handle spaces and special characters
    create_table_query = text(
        f"""
        CREATE TABLE IF NOT EXISTS raw."{table_name}" (
            {', '.join([f'"{col}" TEXT' for col in columns])}
        )
    """
    )

    with engine.connect() as conn:
        conn.execute(create_table_query)


def upload_csv_to_table(engine, table_name, csv_file):
    """
    Upload CSV data to the table in the database.
    """
    df = pd.read_csv(csv_file)
    df.to_sql(
        table_name,
        engine,
        schema="raw",
        if_exists="append",
        index=False,
        method="multi",
    )


def main(directory):
    """
    Main function to upload all CSVs in the directory to the database.
    """
    print("PGDATABASE:", os.getenv("PGDATABASE"))
    print("PGUSER:", os.getenv("PGUSER"))
    print("PGPASSWORD:", os.getenv("PGPASSWORD"))
    print("PGHOST:", os.getenv("PGHOST"))
    print("PGPORT:", os.getenv("PGPORT"))

    db_url = f"postgresql+psycopg2://{os.getenv('PGUSER')}:{os.getenv('PGPASSWORD')}@{os.getenv('PGHOST')}:5432/{os.getenv('PGDATABASE')}"
    engine = create_engine(db_url)

    for filename in os.listdir(directory):
        if filename.endswith(".csv"):
            table_name = os.path.splitext(filename)[
                0
            ]  # Use the file name (without extension) as the table name
            csv_file = os.path.join(directory, filename)

            print(f"Processing file: {csv_file}")

            # Create table if it does not exist
            create_table_from_csv(engine, table_name, csv_file)

            # Upload CSV data to the table
            upload_csv_to_table(engine, table_name, csv_file)

            print(f"Uploaded {filename} to table {table_name}")


if __name__ == "__main__":
    directory = "data/PurpleAir_Cleaned_1"  # Replace with the path to your directory containing CSV files
    main(directory)
