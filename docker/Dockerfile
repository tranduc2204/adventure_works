FROM mcr.microsoft.com/mssql/server:2022-latest

USER root

# Install Python3, pip, and utilities for ingestion
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        python3 \
        python3-pip \
        curl && \
    rm -rf /var/lib/apt/lists/*

# Default environment variables
ENV ACCEPT_EULA=Y
ENV MSSQL_PID=Developer
ENV SA_PASSWORD=YourStrong@Passw0rd
ENV DB_NAME=AdventureWorks

# Set working directory
WORKDIR /usr/src/app

# Install Python dependencies
COPY requirements.txt .
RUN pip3 install --no-cache-dir --break-system-packages -r requirements.txt 2>/dev/null || pip3 install --no-cache-dir -r requirements.txt

# Copy source data, ingestion script, and entrypoint
COPY data/ ./data/
COPY src/ingest.py .
COPY docker/entrypoint.sh ./entrypoint.sh

# Ensure proper permissions
RUN chmod +x entrypoint.sh && \
    chown -R mssql:root /usr/src/app

USER mssql

EXPOSE 1433

ENTRYPOINT ["./entrypoint.sh"]