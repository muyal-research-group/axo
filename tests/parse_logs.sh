#!/bin/bash

docker run --name lp-0 -e SOURCE_PATH=/source  -e SINK_PATH=/sink -e MAX_WORKERS=10 -v  ./log:/source -v ./out:/sink  nachocode/utils:log-parser