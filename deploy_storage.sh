#!/bin/bash

docker network create -d bridge axo-net --subnet 11.0.0.0/25  || true

echo "Stop storage service..."
docker compose -f ./storage.yml -p axo-storage down
echo "Deploying storage service..."
docker compose -f ./storage.yml -p axo-storage up -d
