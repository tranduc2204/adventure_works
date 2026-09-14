#!/usr/bin/env bash
set -e

# ==============================================================================
# Paths & Environment Loading
# ==============================================================================
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# Load .env file (checks root directory)
if [ -f "$ROOT_DIR/.env" ]; then
    echo "Loading configuration from: $ROOT_DIR/.env"
    set -a
    source "$ROOT_DIR/.env"
    set +a
elif [ -f "$SCRIPT_DIR/.env" ]; then
    echo "Loading configuration from: $SCRIPT_DIR/.env"
    set -a
    source "$SCRIPT_DIR/.env"
    set +a
else
    echo "ERROR: File .env not found! Please create .env from .env.example"
    exit 1
fi

# ==============================================================================
# Configuration Validation
# ==============================================================================
if [ -z "$SA_PASSWORD" ]; then
    echo "ERROR: SA_PASSWORD is not set in .env! Please configure your password."
    exit 1
fi

CONTAINER_NAME="${CONTAINER_NAME:-sqlserver_adventureworks}"
PORT="${PORT:-1433}"
DB_NAME="${DB_NAME:-AdventureWorks}"
IMAGE_NAME="${IMAGE_NAME:-adventureworks-sqlserver}"
DATA_DIR="$ROOT_DIR/data"
INGEST_SCRIPT="$ROOT_DIR/src/ingest.py"
DOCKERFILE="$ROOT_DIR/docker/Dockerfile"

echo "================================================================"
echo " Starting SQL Server & Data Ingestion Setup"
echo " Container Name: $CONTAINER_NAME"
echo " Port:           $PORT"
echo " Database:       $DB_NAME"
echo " Data Dir:       $DATA_DIR"
echo " Ingest Script:  $INGEST_SCRIPT"
echo " Dockerfile:     $DOCKERFILE"
echo "================================================================"

# Step 1: Remove existing container if running
if [ "$(docker ps -aq -f name=^/${CONTAINER_NAME}$)" ]; then
    echo "Stopping and removing existing container '$CONTAINER_NAME'..."
    docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true
fi

# Step 2: Build Docker Image and Run Container
echo ""
echo "--- Step 1: Building Docker Image ---"
docker build -f "$DOCKERFILE" -t "$IMAGE_NAME" "$ROOT_DIR"

echo ""
echo "--- Step 2: Starting Container ---"
docker run -d \
    --name "$CONTAINER_NAME" \
    -p "$PORT:1433" \
    -e "ACCEPT_EULA=Y" \
    -e "SA_PASSWORD=$SA_PASSWORD" \
    -e "MSSQL_PID=Developer" \
    -e "DB_NAME=$DB_NAME" \
    "$IMAGE_NAME"

# Step 3: Copy source data and ingest.py to container
echo ""
echo "--- Step 3: Copying Source Data and Ingest Script to Container ---"
docker exec "$CONTAINER_NAME" mkdir -p /usr/src/app/data
docker cp "$DATA_DIR/." "$CONTAINER_NAME:/usr/src/app/data/"
docker cp "$INGEST_SCRIPT" "$CONTAINER_NAME:/usr/src/app/ingest.py"
echo "Successfully copied source data and ingest.py into container."

# Step 4: Wait for SQL Server to be healthy
echo ""
echo "--- Step 4: Waiting for SQL Server on port $PORT ---"
MAX_TRIES=30
COUNT=0
READY=0

while [ $COUNT -lt $MAX_TRIES ]; do
    COUNT=$((COUNT + 1))
    if docker exec "$CONTAINER_NAME" /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "$SA_PASSWORD" -C -Q "SELECT 1" >/dev/null 2>&1; then
        echo "SQL Server is ready (verified via sqlcmd)!"
        READY=1
        break
    fi
    echo "Waiting for SQL Server to accept connections... ($COUNT/$MAX_TRIES)"
    sleep 2
done

if [ $READY -ne 1 ]; then
    echo "ERROR: SQL Server did not become ready in time."
    docker logs "$CONTAINER_NAME" | tail -n 20
    exit 1
fi

# Step 5: Execute Ingestion Script
echo ""
echo "--- Step 5: Running Ingestion (CSV -> SQL Server) ---"
if [ -f "$ROOT_DIR/venv/bin/python" ]; then
    echo "Running ingest.py using host venv against localhost:$PORT..."
    "$ROOT_DIR/venv/bin/python" "$INGEST_SCRIPT" \
        --host localhost \
        --port "$PORT" \
        --user sa \
        --password "$SA_PASSWORD" \
        --database "$DB_NAME" \
        --data-dir "$DATA_DIR"
else
    echo "Running ingest.py inside container..."
    docker exec "$CONTAINER_NAME" python3 /usr/src/app/ingest.py \
        --host localhost \
        --port 1433 \
        --user sa \
        --password "$SA_PASSWORD" \
        --database "$DB_NAME" \
        --data-dir /usr/src/app/data
fi

echo ""
echo "================================================================"
echo " Setup Completed Successfully!"
echo " SQL Server is running at: localhost:$PORT"
echo " Database:                 $DB_NAME"
echo " User:                     sa"
echo " Password:                 $SA_PASSWORD"
echo "================================================================"
