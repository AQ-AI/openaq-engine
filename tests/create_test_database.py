import psycopg2
from sqlalchemy import create_engine, text


def setup_test_database():
    # Retrieve superuser connection details from environment variables
    superuser = "openaq"
    superuser_password = "openaq"

    # Connect to the default postgres database to create the test_db
    try:
        con = psycopg2.connect(
            dbname="postgres",
            user=superuser,
            host="localhost",
            password=superuser_password,
        )
        con.autocommit = True
        cur = con.cursor()
        cur.execute("CREATE DATABASE test_db")
        cur.close()
        con.close()
    except psycopg2.errors.DuplicateDatabase:
        print(
            "Database creation might have failed (or it already exists). Continuing..."
        )

    # Connect to the test_db as superuser
    test_db_url = (
        f"postgresql://{superuser}:{superuser_password}@localhost:5432/test_db"
    )
    test_engine = create_engine(test_db_url, isolation_level="AUTOCOMMIT")

    with test_engine.connect() as connection:
        # Debug: Check the connection
        result = connection.execute(text("SELECT 1"))
        print(f"Connection test result: {result.scalar()}")

        # Create the test_results table
        connection.execute(
            text(
                """
            CREATE TABLE IF NOT EXISTS test_results (
                id SERIAL PRIMARY KEY,
                model_id VARCHAR(50) NOT NULL,
                run_date TIMESTAMP NOT NULL,
                metric_name VARCHAR(50) NOT NULL,
                metric_value DOUBLE PRECISION NOT NULL
            )
        """
            )
        )
        print("Table test_results created successfully.")

        # Create the features table
        connection.execute(
            text(
                """
            CREATE TABLE IF NOT EXISTS features (
                location_id INT NOT NULL,
                cohort VARCHAR(50) NOT NULL,
                timestamp_utc TIMESTAMP NOT NULL,
                PRIMARY KEY (location_id, timestamp_utc)
            )
        """
            )
        )
        print("Table features created successfully.")

        # Create the cohorts_Mumbai table
        connection.execute(
            text(
                """
            CREATE TABLE IF NOT EXISTS cohorts_Mumbai (
                x DOUBLE PRECISION NOT NULL,
                y DOUBLE PRECISION NOT NULL,
                value DOUBLE PRECISION NOT NULL,
                datetime_hour TIMESTAMP NOT NULL
            )
        """
            )
        )
        print("Table cohorts_Mumbai created successfully.")

        # Grant all privileges on the test_db to test_user
        connection.execute(
            text("GRANT ALL PRIVILEGES ON DATABASE test_db TO test_user")
        )
        print("Granted all privileges on test_db to test_user.")

        # Grant all privileges on all tables in test_db to test_user
        connection.execute(
            text(
                "GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO test_user"
            )
        )
        print("Granted all privileges on all tables in test_db to test_user.")

        # Insert example data into test_results table
        connection.execute(
            text(
                """
            INSERT INTO test_results (model_id, run_date, metric_name, metric_value) VALUES
            ('model_1', '2024-01-01 00:00:00', 'R2', 0.9),
            ('model_1', '2024-01-01 00:00:00', 'MSE', 0.1),
            ('model_1', '2024-01-01 00:00:00', 'MAPE', 0.05),
            ('model_2', '2024-01-02 00:00:00', 'R2', 0.85),
            ('model_2', '2024-01-02 00:00:00', 'MSE', 0.2),
            ('model_2', '2024-01-02 00:00:00', 'MAPE', 0.04)
        """
            )
        )
        print("Data inserted into test_results table successfully.")

        # Verify that the data is inserted
        result = connection.execute(text("SELECT * FROM test_results"))
        rows = result.fetchall()
        print(f"Inserted rows: {rows}")


if __name__ == "__main__":
    setup_test_database()
