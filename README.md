[![GoDoc](https://godoc.org/github.com/m-lab/ndt-server?status.svg)](https://godoc.org/github.com/m-lab/ndt-server) [![Build Status](https://app.travis-ci.com/m-lab/ndt-server.svg?branch=main)](https://travis-ci.com/m-lab/ndt-server) [![Coverage Status](https://coveralls.io/repos/github/m-lab/ndt-server/badge.svg?branch=main)](https://coveralls.io/github/m-lab/ndt-server?branch=main) [![Go Report Card](https://goreportcard.com/badge/github.com/m-lab/ndt-server)](https://goreportcard.com/report/github.com/m-lab/ndt-server)

# TurboTest real-time early termination

This repository adds TurboTest to the M-Lab ndt7 server. TurboTest stops an
ndt7 download test early when a model predicts that the final throughput is
known. It uses the two-stage model of the TurboTest paper
([NSDI '26](https://www.usenix.org/system/files/nsdi26-manda.pdf)):

* **Stage 1:** an XGBoost regressor predicts the throughput from the TCP
  statistics of the test.
* **Stage 2:** a Transformer classifier for each epsilon (5, 10, ..., 35)
  decides from the TCP statistics and all the Stage 1 predictions if the test
  can stop. A higher epsilon stops sooner with a larger error.

The test runs as a normal ndt7 test. TurboTest only tells the server, through
Redis, when to stop. If Redis or the inference service is not available, the
test runs to its normal length.

## Repository layout

| Path | Component | Language |
|---|---|---|
| `.` | ndt-server with the TurboTest integration (`ndt7/handler/turbotest.go`, `redis/`) | Go |
| `tcp-info/` | tcp-info that streams every tcp_info snapshot of ndt7 connections to Redis (`redis/client.go`) | Go |
| `turbotest/` | the real-time inference service, deploy scripts, tests and evaluation tools | Python |
| `ndt7-client/` | ndt7 client with the `-turbotest` and `-turbotest-epsilon` flags | Go |

`tcp-info/` and `ndt7-client/` are separate Go modules. They come from
[tcp-info-Turbotest-implementation](https://github.com/haarikamanda/tcp-info-Turbotest-implementation)
(commit `f720bf4`) and
[ndt7-client-go-snl](https://github.com/haarikamanda/ndt7-client-go-snl)
(branch `netreplica-branch`, commit `3efbcc7`).

## Architecture

```
ndt7 client --(?turbotest=enforce|shadow&turbotest_epsilon=E)--> ndt-server
                                                                  |  RPUSH turbotest:queue {uuid, mode, epsilon}
                                                                  |  SET table_2:<uuid> 0
                                                                  v
tcp-info --RPUSH table_1:<uuid> (one row per snapshot, ~10 ms)--> Redis <--+
                                                                  ^        |
TurboTest service: reads table_1 every 5 ms, 100 ms feature rows,          |
  Stage 1 + Stage 2 every 0.5 s (tau = 5, 10, ..., 95 rows)                |
  stop: SET table_2:<uuid> 1, HSET turbotest:result:<uuid> ----------------+
                                                                  |
ndt-server: reads all table_2 flags every 10 ms (one MGET)        |
  enforce: stops sending, sends the prediction in the final message, closes
```

| Redis key | Type | Writer | Content |
|---|---|---|---|
| `turbotest:queue` | list | ndt-server | one JSON job for each TurboTest download |
| `table_1:<uuid>` | list | tcp-info | `ts_ns,BytesAcked,RTT,SndCwnd,BytesSent,BytesRetrans,Retransmits,CwndGain` |
| `table_2:<uuid>` | string | ndt-server (0), service (1) | the stop flag |
| `turbotest:result:<uuid>` | hash | service | `tau`, `predicted_mbps`, `probability`, timings |
| `turbotest:shadow:<uuid>` | hash | service | shadow mode: the decision of every epsilon |
| `turbotest:done:<uuid>` | string | ndt-server | the download ended |

Modes:

* `enforce`: the test stops when the classifier says stop. The client gets the
  Stage 1 prediction in the final measurement (`TurboTest` field).
* `shadow`: the test runs to its full length. The service records the
  decision of every epsilon on the same traffic. Use this mode to measure
  accuracy.

## Requirements

* Linux 4.19 or later, Docker 20.10 or later.
* Go 1.22 or later, to build the ndt7 client on the host.
* Python 3.10 and Redis 6 or later, only to run the service or the tools
  without Docker.
* The TurboTest model files (not in this repository). The service reads them
  from a `model_shared` directory with this layout:

```
model_shared/
  xgboost_regressor/model/saved_paths_08_28_2025_throughput_5.3/
    xg_regressor_5.pkl
    xg_all_scaler_5.pkl
  transformer_classifier/
    saved_normalization_values/min_vals.npy, ranges.npy
    model_paths/2025_09_05_classifier_BCE_oracle_<eps>_regressor_output_epoch5*.pt
```

## Setup

Run all commands from the repository root.

1. Build the three images:

   ```
   docker build -t tcp-info-custom tcp-info
   docker build -t ndt-fullstack:custom .
   docker build -t turbotest-service turbotest
   ```

   The `ndt-fullstack` image takes the tcp-info binary from the
   `tcp-info-custom` image, so build `tcp-info-custom` first.

2. Start the stack (Redis, ndt-server with tcp-info, and the service):

   ```
   MODEL_DIR=/path/to/model_shared turbotest/deploy/run_stack.sh
   ```

   The script waits until all parts are ready. The ndt7 port is
   `ws://localhost:18080`. It binds to 127.0.0.1 only. To serve remote
   clients, set `NDT_BIND=0.0.0.0`. Other settings: `NDT_HTTP_PORT`,
   `REDIS_HOST_PORT` (16379), `METRICS_HOST_PORT` (19994, Prometheus),
   `CERT_DIR` (wss on port 14443).

3. Build the client:

   ```
   (cd ndt7-client && go build -o ndt7-client ./cmd/ndt7-client)
   ```

4. Run a test:

   ```
   # TurboTest stops the test (epsilon 15)
   ndt7-client/ndt7-client -server localhost:18080 -scheme ws -upload=false -turbotest enforce -turbotest-epsilon 15
   # full-length test; the service records every epsilon
   ndt7-client/ndt7-client -server localhost:18080 -scheme ws -upload=false -turbotest shadow
   # normal ndt7 test, no TurboTest
   ndt7-client/ndt7-client -server localhost:18080 -scheme ws -upload=false
   ```

   The summary shows the TurboTest decision and the predicted throughput.
   `-format json` gives the `TurboTest` field of the final measurement.

5. Stop the stack:

   ```
   turbotest/deploy/run_stack.sh stop
   ```

## Configuration

ndt-server flags:

| Flag | Default | Description |
|---|---|---|
| `-turbotest.redis-addr` | `$REDIS_ADDR` | Redis address. Empty disables TurboTest. |
| `-turbotest.poll-interval` | 10ms | how often the server reads the stop flags |

tcp-info writes to Redis only if `REDIS_ADDR` is set. `REDIS_SRC_PORTS`
limits the stream to the ndt7 ports.

The service options (`--epsilons`, `--threshold`, `--poll-ms`, `--max-jobs`,
and more) are in [`turbotest/README.md`](turbotest/README.md).

## Tests and evaluation

```
# Go
go test ./ndt7/... ./netx/... ./redis/...
(cd tcp-info && go test ./redis/... ./saver/...)
(cd ndt7-client && go test ./...)

# Python (from turbotest/)
python -m venv .venv && .venv/bin/pip install -r requirements.txt
TURBOTEST_MODEL_DIR=/path/to/model_shared .venv/bin/python -m pytest tests
.venv/bin/python -m tools.parity_check --model-dir /path/to/model_shared
.venv/bin/python -m tools.offline_eval --model-dir /path/to/model_shared --n 20000 --epsilons 5,10,15,20,25,30,35
```

* `tools/parity_check.py` checks that the real-time features and model inputs
  are identical to the offline preprocessing.
* `tools/offline_eval.py` runs the real-time code on the recorded tests and
  gives the median error and the data transferred for each epsilon (the
  metrics of the paper).
* `tools/live_eval.py` runs real baseline, shadow and enforce tests against a
  running stack. `tools/live_report.py` makes the report.

### Live tests with a bottleneck (NetForge)

[NetForge](https://github.com/SNL-UCSB/netforge) puts a shaped link between
the client and the server:

```
git clone https://github.com/SNL-UCSB/netforge /path/to/netforge
docker build -t turbotest-netforge -f turbotest/deploy/netforge/Dockerfile /path/to/netforge/src
NETFORGE_DIR=/path/to/netforge CLIENT=ndt7-client/ndt7-client turbotest/deploy/netforge/run_netforge.sh

cd turbotest
SERVER=$(docker inspect -f '{{(index .NetworkSettings.Networks "ndt-net").IPAddress}}' ndt-fullstack)
python -m tools.live_eval --client /ndt7-client --netforge turbotest-netforge --server $SERVER:18080 --out results/live_netforge
python -m tools.live_report results/live_netforge/live_tests.jsonl
```

The default sweep is 10, 20, 50, 100, 200, 300 and 400 Mbit/s with fq_codel
and 20 ms latency. Results go to `turbotest/results/`, which git ignores.

---

# ndt-server

This repository contains a [ndt5](
https://github.com/ndt-project/ndt/wiki/NDTProtocol) and [ndt7](
spec/ndt7-protocol.md) server written in Go. This code may compile under
many systems, including macOS and Windows, but is specifically designed
and tested for running on Linux 4.19+.

## Clients

Depending on your needs, there are several ways to perform a client measurement
using the NDT7 protocol.

Officially supported by members of M-Lab staff.

* https://github.com/m-lab/ndt7-js (javascript)
* https://github.com/m-lab/ndt7-client-go (golang)

Unofficially supported by members of the M-Lab community.

* https://github.com/m-lab/ndt7-client-cc (c++)
* https://github.com/m-lab/ndt7-client-ios (swift)
* https://github.com/m-lab/ndt7-client-android (kotlin)
* https://github.com/m-lab/ndt7-client-android-java (java)

## Setup

### Primary setup & running (Linux)

Prepare the runtime environment

```bash
install -d certs datadir
```

To run the server locally, generate local self signed certificates (`key.pem`
and `cert.pem`) using bash and OpenSSL

```bash
./gen_local_test_certs.bash
```

build the docker container for `ndt-server`

```bash
docker build . -t ndt-server
```

enable BBR (with which ndt7 works much better)

```
sudo modprobe tcp_bbr
```

and run the `ndt-server` binary container

```bash
docker run --network=bridge                \
           --publish 443:4443              \
           --publish 80:8080               \
           --volume `pwd`/certs:/certs:ro  \
           --volume `pwd`/datadir:/datadir \
           --read-only                     \
           --user `id -u`:`id -g`          \
           --cap-drop=all                  \
           ndt-server                      \
           -cert /certs/cert.pem           \
           -key /certs/key.pem             \
           -datadir /datadir               \
           -ndt7_addr :4443                \
           -ndt7_addr_cleartext :8080
```

### Alternate setup & running (Windows & MacOS)

These instructions assume you have Docker for Windows/Mac installed.

**Note: NDT5 does not work on Docker for Windows/Mac as it requires using the host's network, which is only supported on Linux**

```
docker-compose run ndt-server ./gen_local_test_certs.bash
docker-compose up
```

After making changes you will have to run `docker-compose up --build` to rebuild the ntd-server binary.

## Accessing the service

Once you have done that, you should have a ndt5 server running on ports
`3001` (legacy binary flavour), `3002` (WebSocket flavour), and `3010`
(secure WebSocket flavour); a ndt7 server running on port `443` (over TLS
and using the ndt7 WebSocket protocol); and Prometheus metrics available
on port `9990`.

Try accessing these URLs in your browser (for URLs using HTTPS, certs will
appear invalid to your browser, but everything is safe because this is a test
deployment, hence you should ignore this warning and continue):

* ndt7: http://localhost/ndt7.html or https://localhost/ndt7.html
* ndt5+wss: https://localhost:3010/widget.html
* prometheus: http://localhost:9090/metrics

Replace `localhost` with the IP of the server to access them externally.
