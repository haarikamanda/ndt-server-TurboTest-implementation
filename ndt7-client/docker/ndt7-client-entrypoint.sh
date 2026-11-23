#!/bin/sh
set -e

LOG_DIR=${LOG_DIR:-/logs}
EXPERIMENT=${EXPERIMENT:-default}
NDT7_SCHEME=${NDT7_SCHEME:-ws}
NDT7_SERVER=${NDT7_SERVER:-127.0.0.1:18080}

mkdir -p "$LOG_DIR"
LOG_FILE="${LOG_DIR}/ndt7_raw_${EXPERIMENT}.log"

if [ "$#" -eq 0 ]; then
  set -- --no-verify
fi

HAS_EXTRA=0
HAS_SERVER=0
HAS_SCHEME=0
for arg in "$@"; do
  case "$arg" in
    --extra_metadata)
      HAS_EXTRA=1
      ;;
    --server)
      HAS_SERVER=1
      ;;
    --scheme)
      HAS_SCHEME=1
      ;;
  esac
done

if [ "$HAS_SCHEME" -eq 0 ]; then
  set -- "$@" --scheme "$NDT7_SCHEME"
fi

if [ "$HAS_SERVER" -eq 0 ]; then
  set -- "$@" --server "$NDT7_SERVER"
fi

if [ "$HAS_EXTRA" -eq 0 ]; then
  set -- "$@" --extra_metadata "$EXPERIMENT"
fi

TMP_PIPE=$(mktemp /tmp/ndt7-client.XXXXXX)
rm -f "$TMP_PIPE"
mkfifo "$TMP_PIPE"

tee -a "$LOG_FILE" < "$TMP_PIPE" &
TEE_PID=$!

set +e
/app/ndt7-client "$@" > "$TMP_PIPE" 2>&1
STATUS=$?
set -e

wait "$TEE_PID"
rm -f "$TMP_PIPE"

exit "$STATUS"
