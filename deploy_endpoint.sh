#!/bin/bash
echo "Stop axo-endpoint..."
docker compose -f axo-endpoint.yml down
echo "Deploying axo-endpoint ..."
docker compose -f axo-endpoint.yml up -d  