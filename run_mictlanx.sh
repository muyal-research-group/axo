#!/bin/bash

docker network create -d bridge mictlanx --subnet 10.0.0.0/25  || true

docker compose -f ./mictlanx.yml -p mictlanx up -d
