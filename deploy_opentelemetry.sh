#!/bin/bash

echo "Stop opentelemetry service..." 
docker compose -f ./opentelemetry/docker-compose.yml down
echo "Deploying opentelemetry service..."
docker compose -f ./opentelemetry/docker-compose.yml up -d

