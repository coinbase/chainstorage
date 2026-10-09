#!/bin/sh
set -e

echo "🚀 Initializing PostgreSQL for local Chainstorage development..."

# --- Configuration ---
# Master credentials (from Docker environment)
MASTER_USER="${POSTGRES_USER:-postgres}"
MASTER_PASSWORD="${POSTGRES_PASSWORD:-postgres}"

# Shared passwords for all network-specific roles (from Docker environment)
WORKER_PASSWORD="${CHAINSTORAGE_WORKER_PASSWORD:-worker_password}"
SERVER_PASSWORD="${CHAINSTORAGE_SERVER_PASSWORD:-server_password}"

# List of all networks to create. Format: <blockchain>_<network>
NETWORKS="
ethereum_mainnet
ethereum_goerli
ethereum_holesky
bitcoin_mainnet
base_mainnet
base_goerli
arbitrum_mainnet
polygon_mainnet
polygon_testnet
solana_mainnet
aptos_mainnet
avacchain_mainnet
bsc_mainnet
fantom_mainnet
optimism_mainnet
tron_mainnet
story_mainnet
dogecoin_mainnet
litecoin_mainnet
bitcoincash_mainnet
dash_mainnet
"

# --- Helper Functions ---
# Function to execute a SQL command against the master 'postgres' database.
# It uses the default Unix socket connection which is most reliable for init scripts.
psql_master() {
    PGPASSWORD="$MASTER_PASSWORD" psql -v ON_ERROR_STOP=1 -U "$MASTER_USER" -d "postgres" --no-password -c "$1"
}

# Function to execute a SQL command against a specific network database.
psql_network() {
    local db_name=$1
    local sql_command=$2
    PGPASSWORD="$MASTER_PASSWORD" psql -v ON_ERROR_STOP=1 -U "$MASTER_USER" -d "$db_name" --no-password -c "$sql_command"
}

# --- Main Logic ---
# The official postgres entrypoint script executes files in /docker-entrypoint-initdb.d/
# after the server is initialized but before it's opened for general connections.
# This means we don't need a separate wait loop; the server is ready for us.
echo "✅ PostgreSQL server is ready for initialization."
echo ""

# Loop through all networks to create databases and roles
for network in $NETWORKS; do
  if [ -n "$network" ]; then
    db_name="chainstorage_$network"
    worker_user="cs_${network}_worker"
    server_user="cs_${network}_server"
    
    echo "📦 Setting up: $db_name"

    # --- Create Roles (if they don't exist) ---
    echo "   - Creating role: $worker_user"
    psql_master "DO \$\$ BEGIN IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = '$worker_user') THEN CREATE ROLE \"$worker_user\" WITH LOGIN PASSWORD '$WORKER_PASSWORD'; ELSE ALTER ROLE \"$worker_user\" WITH PASSWORD '$WORKER_PASSWORD'; END IF; END \$\$;"
    
    echo "   - Creating role: $server_user"
    psql_master "DO \$\$ BEGIN IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = '$server_user') THEN CREATE ROLE \"$server_user\" WITH LOGIN PASSWORD '$SERVER_PASSWORD'; ELSE ALTER ROLE \"$server_user\" WITH PASSWORD '$SERVER_PASSWORD'; END IF; END \$\$;"
    
    # --- Create Database (if it doesn't exist) ---
    echo "   - Creating database: $db_name"
    # Use a trick to create database if not exists, since "CREATE DATABASE IF NOT EXISTS" is not available
    if ! psql_master "SELECT 1 FROM pg_database WHERE datname = '$db_name'" | grep -q 1; then
        psql_master "CREATE DATABASE \"$db_name\" OWNER \"$worker_user\";"
    else
        echo "     Database $db_name already exists, skipping creation."
    fi

    # --- Grant Permissions ---
    echo "   - Granting permissions..."
    # Grant connect to the server user on the new database
    psql_master "GRANT CONNECT ON DATABASE \"$db_name\" TO \"$server_user\";"
    
    # Connect to the new database to set schema permissions
    # Grant server read-only access
    psql_network "$db_name" "GRANT USAGE ON SCHEMA public TO \"$server_user\";"
    psql_network "$db_name" "GRANT SELECT ON ALL TABLES IN SCHEMA public TO \"$server_user\";"
    psql_network "$db_name" "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO \"$server_user\";"
    
    # Grant worker full access
    psql_network "$db_name" "GRANT ALL PRIVILEGES ON SCHEMA public TO \"$worker_user\";"
    psql_network "$db_name" "GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO \"$worker_user\";"
    psql_network "$db_name" "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL PRIVILEGES ON TABLES TO \"$worker_user\";"

    echo "   - ✅ Setup complete for $db_name"
    echo ""
  fi
done

echo "🎉 All network databases initialized successfully for local development!"
echo ""
echo "📋 Summary of Shared Credentials:"
echo "   - All worker roles (e.g., cs_ethereum_mainnet_worker) use password: '$WORKER_PASSWORD'"
echo "   - All server roles (e.g., cs_ethereum_mainnet_server) use password: '$SERVER_PASSWORD'"
echo ""
echo "🚀 Ready to start Chainstorage!" 