#!/bin/bash

docker network create -d bridge axo-net --subnet 11.0.0.0/25  || true

docker compose -f ./storage.yml -p axo-storage up -d
