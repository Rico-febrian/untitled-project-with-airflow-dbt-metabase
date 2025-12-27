#!/bin/bash

# =================================================================
# DEV SETUP SCRIPT
# Fungsi: Restart service tanpa hapus data (Volume tetep aman)
# =================================================================

echo "Starting Development Environment..."

# 1. Start Infrastructure Dasar (DB & Storage)
echo "Starting Warehouse and Data Lake..."
docker compose -f ./setup/warehouse/docker-compose.yml up -d
docker compose -f ./setup/data-lake/docker-compose.yml up -d

# 2. Start Supporting Services
echo "Starting Data Sources and Monitoring..."
docker compose -f ./setup/sources/docker-compose.yml up -d
docker compose -f ./setup/airflow-monitoring/docker-compose.yml up -d

# 3. Start Airflow (The Core)
echo "Starting Airflow..."
docker compose -f ./setup/airflow/docker-compose.yml up -d

# 4. Sinkronisasi Ulang
echo "Refreshing Airflow Connections and Variables..."
docker exec airflow-webserver airflow connections import /init/variables-and-connections/airflow-connections-init.yaml
docker exec airflow-webserver airflow variables import -a overwrite /init/variables-and-connections/airflow-variables-init.json

echo "All services are up! Check Grafana at :3000 and Airflow at :8080"