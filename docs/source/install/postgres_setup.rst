
Local Postgres setup
=========================

Create Postgres User and database;
-----------------------------------

.. code-block:: bash

    psql -U postgres

This only needs to be done once, skip ahead to Login

.. code-block:: psql

    CREATE ROLE openaq WITH LOGIN PASSWORD 'openaq';
    CREATE DATABASE openaq_db;
    GRANT ALL PRIVILEGES ON DATABASE openaq_db TO openaq;
    ALTER ROLE openaq SUPERUSER;
    SELECT pg_reload_conf();


Restart postgres:

.. code-block:: bash

    sudo systemctl restart postgresql-12.service

And Login:

.. code-block:: psql

    psql -U openaq -d openaq_db -h localhost -W
