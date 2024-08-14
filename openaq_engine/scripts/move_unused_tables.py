import psycopg2


def move_tables(conn, current_schema, new_schema, tables):
    with conn.cursor() as cur:
        for table in tables:
            query = f"""
            ALTER TABLE {current_schema}.{table} SET SCHEMA {new_schema};
            """
            cur.execute(query)
            print(f"Moved {current_schema}.{table} to {new_schema}.{table}")
    conn.commit()


# Database connection parameters
conn_params = {
    "dbname": "your_database",
    "user": "your_username",
    "password": "your_password",
    "host": "your_host",
    "port": "your_port",
}

# Tables to move
tables_to_move = ["table1", "table2", "table3"]

# Connect to the database
conn = psycopg2.connect(**conn_params)

# Move the tables
move_tables(conn, "public", "old", tables_to_move)

# Close the connection
conn.close()
