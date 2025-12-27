#!/bin/bash

# Start all airflow services
docker compose -f ./setup/airflow/docker-compose.yml down -v
docker compose -f ./setup/airflow/docker-compose.yml up --build --detach --force-recreate

# Start monitoring services
docker compose -f ./setup/airflow-monitoring/docker-compose.yml down -v
docker compose -f ./setup/airflow-monitoring/docker-compose.yml up --build --detach --force-recreate

# Start data lake services
docker compose -f ./setup/data-lake/docker-compose.yml down -v
docker compose -f ./setup/data-lake/docker-compose.yml up --build --detach --force-recreate

# Start data warehouse services
docker compose -f ./setup/warehouse/docker-compose.yml down -v
docker compose -f ./setup/warehouse/docker-compose.yml up --build --detach --force-recreate

# Import airflow variables and connections
docker exec -it airflow-webserver airflow connections import /init/variables-and-connections/airflow-connections-init.yml
docker exec -it airflow-webserver airflow variables import -a overwrite /init/variables-and-connections/airflow-variables-init.json
