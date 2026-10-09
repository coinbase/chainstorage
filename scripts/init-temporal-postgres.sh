#!/bin/sh
set -e

echo "Setting up PostgreSQL for Temporal..."

# Since temporal is already the main PostgreSQL user, we just need to create the databases
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-'EOSQL'
  -- Grant CREATEDB privilege to temporal so it can create additional databases
  ALTER ROLE temporal CREATEDB;

  -- Create temporal_visibility database (temporal database already exists as default)
  CREATE DATABASE temporal_visibility OWNER temporal;

  -- Grant all privileges to temporal role on both databases
  GRANT ALL PRIVILEGES ON DATABASE temporal TO temporal;
  GRANT ALL PRIVILEGES ON DATABASE temporal_visibility TO temporal;
EOSQL

echo "✅ Temporal PostgreSQL setup complete!"
echo "Databases: temporal, temporal_visibility"
echo "Roles: temporal (CREATEDB + full access)" 