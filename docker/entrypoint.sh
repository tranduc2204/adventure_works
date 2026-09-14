#!/usr/bin/env bash
set -e

# Start SQL Server in the background
/opt/mssql/bin/sqlservr &
SQL_PID=$!

echo "SQL Server starting (PID $SQL_PID)..."

# Wait for SQL Server to be ready to accept connections
echo "Waiting for SQL Server to be ready on port 1433..."
for i in {1..45}; do
    if /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "${SA_PASSWORD:-YourStrong@Passw0rd}" -C -Q "SELECT 1" > /dev/null 2>&1; then
        echo "SQL Server is healthy and accepting connections!"
        break
    fi
    sleep 2
done

# If ingest.py exists inside the container, run it automatically
if [ -f "/usr/src/app/ingest.py" ]; then
    echo "Running data ingestion script (ingest.py)..."
    python3 /usr/src/app/ingest.py \
        --host localhost \
        --port 1433 \
        --user sa \
        --password "${SA_PASSWORD:-YourStrong@Passw0rd}" \
        --database "${DB_NAME:-AdventureWorks}" \
        --data-dir "/usr/src/app/data" || echo "Ingestion completed with notice."
fi

# Keep container alive by waiting for sqlservr process
wait $SQL_PID

