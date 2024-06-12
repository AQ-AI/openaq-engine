import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


def cleanup_test_database():
    # Retrieve superuser connection details from environment variables
    superuser = "openaq"
    superuser_password = "openaq"

    # Connect to the default postgres database to drop the test_db
    try:
        con = psycopg2.connect(
            dbname="postgres",
            user=superuser,
            host="localhost",
            password=superuser_password,
        )
        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = con.cursor()
        cur.execute("DROP DATABASE IF EXISTS test_db")
        cur.close()
        con.close()
        print("Dropped database test_db.")
    except Exception as e:
        print(f"Error occurred: {e}")


if __name__ == "__main__":
    cleanup_test_database()
