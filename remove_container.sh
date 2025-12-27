#!/bin/bash

# Delete all services and their volumes
docker compose -f ./setup/airflow/docker-compose.yml down -v
docker compose -f ./setup/airflow-monitoring/docker-compose.yml down -v
docker compose -f ./setup/data-lake/docker-compose.yml down -v
docker compose -f ./setup/warehouse/docker-compose.yml down -v
