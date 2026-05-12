#!/bin/bash
# Starts SQL Server, waits until it accepts connections, then runs the init
# script to create the database and enable Change Tracking.
#
# This is used as the custom entrypoint in docker-compose so that the init SQL
# runs inside the same container that SQL Server runs in — no separate
# init-container needed.

set -e

INIT_SQL=/init.sql
SQLCMD=/opt/mssql-tools18/bin/sqlcmd

echo "[entrypoint] Starting SQL Server..."
/opt/mssql/bin/sqlservr &
SQLSERVER_PID=$!

echo "[entrypoint] Waiting for SQL Server to accept connections..."
for i in $(seq 1 60); do
    $SQLCMD -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -No \
            -Q "SELECT 1" > /dev/null 2>&1 && break
    echo "[entrypoint] Attempt $i/60 — not ready yet, retrying in 2s..."
    sleep 2
done

# Final check — abort if still not up
$SQLCMD -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -No \
        -Q "SELECT 1" > /dev/null 2>&1 || {
    echo "[entrypoint] ERROR: SQL Server did not start within 120 s. Aborting."
    exit 1
}

echo "[entrypoint] SQL Server is up. Running init script..."
$SQLCMD -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -No -i $INIT_SQL

echo "[entrypoint] Init complete. SQL Server is ready."

# Bring SQL Server back to the foreground so Docker tracks the process
wait $SQLSERVER_PID
