#!/bin/bash
readonly URL=${1:-http://localhost:60666}
readonly s=${2:-0}
readonly l=${3:-1}
curl_response=$(curl --request GET \
	--url "${URL}/api/v4/peers/stats?start=${s}&end=${l}")

python3 -c "import json, sys; print(json.dumps(json.loads(sys.argv[1]), indent=4))" "$curl_response"
