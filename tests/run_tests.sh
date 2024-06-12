#!/bin/bash

# Set environment variables for test database
export TEST_PGDATABASE=test_db
export TEST_PGUSER=test_user
export TEST_PGPASSWORD=test_password
export TEST_PGHOST=localhost
export PGPORT=5432

# Run the setup script to create the test database and populate it with test data
python create_test_database.py

# Check if the setup was successful
if [ $? -ne 0 ]; then
  echo "Database setup failed. Exiting."
  exit 1
fi

# Run the tests using poetry
poetry run pytest --cov -vv

# Capture the exit code of pytest
TEST_EXIT_CODE=$?

# Optionally clean up the test database
python cleanup_test_database.py

# Exit with the same code as pytest
exit $TEST_EXIT_CODE
