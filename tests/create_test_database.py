import os

import psycopg2
from sqlalchemy import create_engine, text


def setup_test_database():
    # Retrieve superuser connection details from test-specific environment variables
    superuser = os.getenv(
        "PGUSER",
    )
    superuser_password = os.getenv(
        "PGPASSWORD",
    )
    pg_host = os.getenv("TEST_PGHOST", "localhost")
    pg_port = os.getenv("TEST_PGPORT", "5432")
    test_db_name = os.getenv("TEST_PGDATABASE", "test_db")
    test_user = os.getenv("TEST_PGUSER", "test_user")

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
        cur.execute(f"CREATE DATABASE {test_db_name}")
        cur.close()
        con.close()
    except psycopg2.errors.DuplicateDatabase:
        print(f"Database '{test_db_name}' already exists. Continuing...")

    # Connect to the test_db as superuser
    test_db_url = f"postgresql://{superuser}:{superuser_password}@{pg_host}:{pg_port}/{test_db_name}"
    test_engine = create_engine(test_db_url, isolation_level="AUTOCOMMIT")

    with test_engine.connect() as connection:
        # Debug: Check the connection
        result = connection.execute(text("SELECT 1"))
        print(f"Connection test result: {result.scalar()}")

        # Drop existing tables if they exist
        connection.execute(
            text(
                "DROP TABLE IF EXISTS test_results, features, cohorts_mumbai, modis_061_mcd19a2_granules"
            )
        )

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

        # Create the cohorts_mumbai table
        connection.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS cohorts_mumbai (
                    id SERIAL PRIMARY KEY,
                    train_validation_set INT,
                    cohort VARCHAR(50),
                    cohort_type VARCHAR(50),
                    x FLOAT,
                    y FLOAT,
                    value INT,
                    timestamp_utc TIMESTAMP,
                    date JSON,
                    locationId INT8,
                    location TEXT,
                    parameter TEXT,
                    unit TEXT,
                    coordinates JSON,
                    country TEXT,
                    city TEXT,
                    isMobile BOOL,
                    isAnalysis BOOL,
                    entity TEXT,
                    sensorType TEXT
                )
            """
            )
        )
        print("Table cohorts_mumbai created successfully.")

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

        # Insert example data into cohorts_mumbai table
        connection.execute(
            text(
                """
                INSERT INTO cohorts_mumbai (
                    train_validation_set, cohort, cohort_type, x, y, value, timestamp_utc, date,
                    locationId, location, parameter, unit, coordinates, country, city, isMobile, isAnalysis, entity, sensorType
                ) VALUES
                (0, '0_2023-04-01T21:00:00_2022-04-01T21:00:00', 'training', -70.214134, 44.089355, 10,
                    '2022-04-01 21:00:00.000000Z', '{"utc": "2022-04-01T21:00:00.000Z", "local": "2022-04-01T17:00:00-04:00"}',
                    1, 'Location1', 'pm25', 'µg/m³', '{"latitude": 44.089355, "longitude": -70.214134}', 'IN', 'City1', 'False', 'False', 'government', 'reference grade'
                )            """
            )
        )
        print("Data inserted into cohorts_mumbai table successfully.")

        # Create the modis_061_mcd19a2_granules table
        connection.execute(
            text(
                """
            CREATE TABLE IF NOT EXISTS modis_061_mcd19a2_granules (
                id SERIAL PRIMARY KEY,
                sensor_longitude FLOAT,
                sensor_latitude FLOAT,
                datetime TIMESTAMP,
                Optical_Depth_047 FLOAT,
                SR_B2 FLOAT,
                SR_B3 FLOAT,
                SR_B4 FLOAT
            )
        """
            )
        )
        print('Table "modis_061_mcd19a2_granules" created successfully.')

        # Insert example data into modis_061_mcd19a2_granules table
        connection.execute(
            text(
                """
            INSERT INTO modis_061_mcd19a2_granules (sensor_longitude, sensor_latitude, datetime, Optical_Depth_047, SR_B2, SR_B3, SR_B4) VALUES
            (-70.214134, 44.089355, '2022-04-01 21:00:00.000000Z', 0.174, 13539, 12604, 11023),
            (-70.214134, 44.089355, '2022-05-01 21:00:00.000000Z', 0.180, 13600, 12700, 11100)
        """
            )
        )
        print(
            'Data inserted into "modis_061_mcd19a2_granules" table successfully.'
        )

        # Grant all privileges on the test_db to test_user
        connection.execute(
            text(
                f"GRANT ALL PRIVILEGES ON DATABASE {test_db_name} TO {test_user}"
            )
        )
        print(f"Granted all privileges on {test_db_name} to {test_user}.")

        # Grant all privileges on all tables in test_db to test_user
        connection.execute(
            text(
                "GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO test_user"
            )
        )
        print(
            f"Granted all privileges on all tables in {test_db_name} to {test_user}."
        )

        # Verify that the cohorts_mumbai table exists and is populated
        result = connection.execute(
            text("SELECT COUNT(*) FROM cohorts_mumbai")
        )
        count = result.scalar()
        print(f"Verification: cohorts_mumbai table has {count} rows.")
        assert count > 0, "Verification failed: cohorts_mumbai table is empty."

        result = connection.execute(
            text("SELECT COUNT(*) FROM modis_061_mcd19a2_granules")
        )
        count = result.scalar()
        print(
            f'Verification: "modis_061_mcd19a2_granules" table has {count} rows.'
        )
        assert (
            count > 0
        ), 'Verification failed: "modis_061_mcd19a2_granules" table is empty.'


if __name__ == "__main__":
    setup_test_database()
