#!/bin/bash
# Enable Apache AGE extension and create healthcare ontology graph.
# Uses POSTGRES_DB environment variable for database name compatibility
# with different configurations (dev, e2e, test, production).
set -e

echo "Enabling Apache AGE extension..."

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    -- Enable the AGE extension
    CREATE EXTENSION IF NOT EXISTS age;

    -- Configure search path: public first (for table creation), ag_catalog included (for Cypher functions)
    ALTER DATABASE "$POSTGRES_DB" SET search_path = public, ag_catalog, "\$user";

    -- Set search_path for current session (ALTER DATABASE only affects new connections)
    SET search_path = public, ag_catalog, "\$user";

    -- Load the AGE extension
    LOAD 'age';

    -- Create healthcare ontology graph (idempotent)
    DO \$\$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM ag_catalog.ag_graph WHERE name = 'healthcare_ontology') THEN
            PERFORM ag_catalog.create_graph('healthcare_ontology');
            RAISE NOTICE 'Graph healthcare_ontology created';
        ELSE
            RAISE NOTICE 'Graph healthcare_ontology already exists';
        END IF;
    END \$\$;
EOSQL

echo "Apache AGE enabled successfully for database: $POSTGRES_DB"
