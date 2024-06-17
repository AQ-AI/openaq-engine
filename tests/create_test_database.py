import psycopg2
import os

from sqlalchemy import create_engine, text


def setup_test_database():
    # Retrieve superuser connection details from environment variables
    superuser = os.getenv("PGUSER")
    superuser_password = os.getenv("PGPASSWORD")
    pg_host = os.getenv("PGHOST", "localhost")
    pg_port = os.getenv("PGPORT", "5432")

    # Connect to the default postgres database to create the test_db
    try:
        con = psycopg2.connect(
            dbname="postgres",
            user=superuser,
            host=pg_host,
            port=pg_port,
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
    test_db_url = f"postgresql://{superuser}:{superuser_password}@{pg_host}:{pg_port}/test_db"
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
                id SERIAL PRIMARY KEY,
                feature_name VARCHAR(50) NOT NULL,
                feature_value DOUBLE PRECISION NOT NULL
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
                id SERIAL PRIMARY KEY,
                train_validation_set INT,
                cohort VARCHAR(50),
                cohort_type VARCHAR(50),
                x FLOAT,
                y FLOAT,
                longitude FLOAT,
                latitude FLOAT,
                value FLOAT,
                timestamp_utc TIMESTAMP
            )
        """
            )
        )
        print("Table cohorts_Mumbai created successfully.")

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

        # Insert example data into cohorts_Mumbai table
        connection.execute(
            text(
                """
            INSERT INTO cohorts_Mumbai (train_validation_set, cohort, cohort_type, x, y, longitude, latitude, value, timestamp_utc) VALUES
            (0, 'A', 'training', -70.214134, 44.089355, -70.214134, 44.089355, 10, '2022-04-01 21:00:00'),
            (0, 'A', 'training', -70.214134, 44.089355, -70.214134, 44.089355, 20, '2022-05-01 21:00:00'),
            (1, 'B', 'validation', -70.214134, 44.089355, -70.214134, 44.089355, 30, '2022-06-01 21:00:00'),
            (1, 'B', 'validation', -70.214134, 44.089355, -70.214134, 44.089355, 40, '2022-07-01 21:00:00')
        """
            )
        )
        print("Data inserted into cohorts_Mumbai table successfully.")

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


if __name__ == "__main__":
    setup_test_database()
