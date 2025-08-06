#!/bin/bash
docker compose -f axo-endpoint.yml down
docker compose -f axo-endpoint.yml up -d  