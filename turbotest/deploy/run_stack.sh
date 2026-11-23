#!/usr/bin/env bash
# Start (or restart) the TurboTest real-time stack on one host:
#
#   turbotest-redis    Redis (table_1 / table_2 / job queue)
#   ndt-fullstack      ndt-server (TurboTest-enabled) + tcp-info (Redis output)
#                      + traceroute-caller + packet-headers
#   turbotest-service  Python inference service (all epsilon classifiers)
#
# Usage:
#   MODEL_DIR=/path/to/model_shared ./run_stack.sh            # start
#   ./run_stack.sh stop                                       # stop and remove
#
# Build the images first (from the repository roots):
#   (cd tcp-info-Turbotest-implementation && docker build -t tcp-info-custom .)
#   (cd ndt-server-TurboTest-implementation && docker build -t ndt-fullstack:custom .)
#   (cd ndt-server-TurboTest-implementation/turbotest && docker build -t turbotest-service .)
set -euo pipefail

NET=${NET:-ndt-net}
NDT_HTTP_PORT=${NDT_HTTP_PORT:-18080}       # ndt7 cleartext (ws://)
NDT_PROM_PORT=${NDT_PROM_PORT:-19990}
REDIS_HOST_PORT=${REDIS_HOST_PORT:-16379}   # bound to 127.0.0.1 only
METRICS_HOST_PORT=${METRICS_HOST_PORT:-19994}
EPSILON=${EPSILON:-15}
CERT_DIR=${CERT_DIR:-}                      # optional: dir with cert.pem/key.pem for wss on 14443

stop() {
  docker rm -f turbotest-service ndt-fullstack turbotest-redis >/dev/null 2>&1 || true
}

if [[ "${1:-}" == "stop" ]]; then
  stop
  exit 0
fi

: "${MODEL_DIR:?set MODEL_DIR to the model_shared directory}"
MODEL_DIR=$(cd "$MODEL_DIR" && pwd -P)

stop
docker network inspect "$NET" >/dev/null 2>&1 || docker network create "$NET" >/dev/null

docker run -d --name turbotest-redis --network "$NET" --restart unless-stopped \
  -p 127.0.0.1:"$REDIS_HOST_PORT":6379 \
  redis:7 redis-server --save "" --appendonly no --maxmemory 2gb --maxmemory-policy volatile-ttl >/dev/null

tls_args=()
tls_mount=()
if [[ -n "$CERT_DIR" ]]; then
  tls_mount=(-v "$(cd "$CERT_DIR" && pwd -P)":/certs:ro -p 14443:14443)
  tls_args=(--cert=/certs/cert.pem --key=/certs/key.pem --ndt7_addr=:14443)
fi

# NET_ADMIN only so tests can shape this container's egress with tc (bottleneck tests).
docker run -d --name ndt-fullstack --network "$NET" --restart unless-stopped \
  --cap-add NET_ADMIN \
  -p "$NDT_HTTP_PORT":18080 -p 127.0.0.1:"$NDT_PROM_PORT":19990 \
  -v turbotest-ndt-data:/var/spool/ndt \
  "${tls_mount[@]}" \
  -e REDIS_ADDR=turbotest-redis:6379 \
  ndt-fullstack:custom \
  --ndt7_addr_cleartext=:18080 \
  --prometheusx.listen-address=:19990 \
  "${tls_args[@]}" >/dev/null

docker run -d --name turbotest-service --network "$NET" --restart unless-stopped \
  -p 127.0.0.1:"$METRICS_HOST_PORT":9994 \
  -v "$MODEL_DIR":/models:ro \
  -v turbotest-onnx-cache:/cache \
  -e REDIS_ADDR=turbotest-redis:6379 \
  -e TURBOTEST_EPSILON="$EPSILON" \
  turbotest-service >/dev/null

echo "waiting for services..."
for i in $(seq 1 120); do
  if docker logs turbotest-service 2>&1 | grep -q "service ready" &&
     curl -fsS -o /dev/null "http://localhost:$NDT_HTTP_PORT/"; then
    echo "stack is up: ndt7 at ws://localhost:$NDT_HTTP_PORT, Redis at 127.0.0.1:$REDIS_HOST_PORT"
    exit 0
  fi
  sleep 1
done
echo "stack did not become ready; see: docker logs ndt-fullstack / docker logs turbotest-service" >&2
exit 1
