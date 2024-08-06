#!/bin/bash
readonly URL=${1:-http://localhost:60666}

peersprotocol=("http" "http")
peershostname=("mictlanx-peer-0" "mictlanx-peer-1")
peersport=("7000" "7001")
#peersport=("7001" "10000")
for ((i=0; i<${#peersprotocol[@]}; i++ )); do
	protocol="${peersprotocol[i]}"
	host_name="${peershostname[i]}"
	pport="${peersport[i]}"
	echo "$i $protocol://$host_name:$pport"

	curl --request POST \
	  --url "${URL}/api/v4/peers" \
	  --header 'Content-Type: application/json' \
	  --data '{
	  "protocol":"'"$protocol"'",
	  "hostname":"'"$host_name"'",
	  "port":'"$pport"',
	  "peer_id":"'"$host_name"'"
	}'
done
