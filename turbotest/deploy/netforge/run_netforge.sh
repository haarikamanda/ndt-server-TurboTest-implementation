#!/usr/bin/env bash
# Start a NetForge (netreplica solo) bottleneck next to the TurboTest stack.
# The ndt7 client runs in NetForge's ns1; its traffic crosses the shaped
# bridge (veth2: HTB + AQM, download direction) and veth6 (netem latency),
# then reaches ndt-fullstack over the docker network NET.
#
# Usage: NETFORGE_DIR=/path/to/netforge CLIENT=/path/to/ndt7-client ./run_netforge.sh [stop]
set -euo pipefail
NET=${NET:-ndt-net}
NAME=${NAME:-turbotest-netforge}

docker rm -f "$NAME" >/dev/null 2>&1 || true
[[ "${1:-}" == "stop" ]] && exit 0
: "${NETFORGE_DIR:?set NETFORGE_DIR to the netforge repository}"
: "${CLIENT:?set CLIENT to the ndt7-client binary}"

docker run -d --name "$NAME" --network "$NET" --privileged \
  --sysctl net.ipv4.ip_forward=1 \
  -v "$(cd "$NETFORGE_DIR" && pwd -P)/ctps":/ctps:ro \
  turbotest-netforge >/dev/null
docker exec -w /workspace "$NAME" ./setup.sh >/dev/null
docker cp "$CLIENT" "$NAME":/ndt7-client
SERVER_IP=$(docker inspect -f "{{(index .NetworkSettings.Networks \"$NET\").IPAddress}}" ndt-fullstack)
echo "NetForge ready. Client: docker exec $NAME ip netns exec ns1 /ndt7-client -server $SERVER_IP:18080 -scheme ws"
