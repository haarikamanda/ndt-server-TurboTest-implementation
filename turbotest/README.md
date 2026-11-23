# TurboTest real-time early termination

This directory holds the TurboTest inference service and its tools. The
service stops ndt7 download tests early with the two-stage model of the
TurboTest paper (NSDI '26): an XGBoost throughput regressor (Stage 1) and a
Transformer stopping classifier for each epsilon (Stage 2).

## Data path

```
ndt7 client  --(turbotest=enforce|shadow, turbotest_epsilon=E)-->  ndt-server
ndt-server   --RPUSH turbotest:queue {uuid,mode,epsilon}; SET table_2:<uuid> 0-->  Redis
tcp-info     --RPUSH table_1:<uuid> "ts_ns,BytesAcked,RTT,SndCwnd,BytesSent,BytesRetrans,Retransmits,CwndGain"-->  Redis
service      --reads table_1 every 5 ms, 100 ms bins, Stage 1+2 at every 5th bin-->
             --SET table_2:<uuid> 1 + HSET turbotest:result:<uuid> (prediction)-->  Redis
ndt-server   --polls table_2 every 10 ms; enforce: final message with the prediction, close-->  client
```

| Key | Type | Writer | Content |
|---|---|---|---|
| `turbotest:queue` | list | ndt-server | JSON job per TurboTest download |
| `table_1:<uuid>` | list | tcp-info | one row per archived tcp_info snapshot (TTL 120 s) |
| `table_2:<uuid>` | string | ndt-server (0), service (1) | termination flag |
| `turbotest:result:<uuid>` | hash | service | `status`, `tau`, `predicted_mbps`, `probability`, ... |
| `turbotest:shadow:<uuid>` | hash | service | shadow mode: decision of every epsilon |
| `turbotest:done:<uuid>` | string | ndt-server | download ended |

## Exactness with the offline pipeline

* `turbotest/features.py` reproduces `2_features_extraction_final.py`
  incrementally, bit for bit (pandas Welford std, half-even rounding, empty
  bins skipped). See `tests/test_features.py`.
* `turbotest/models.py` builds the Stage 1 and Stage 2 inputs exactly as
  `1_million_throughput_faster.py` and `1_pre_process_chunked.py` /
  `2_attention_test_classifier_regressor_outputs.py` do: 100% identical to the
  preprocessed arrow inputs (`tools/parity_check.py`).
* Stage 2 runs in ONNX Runtime (exported once from the `.pt` checkpoints;
  max |p_onnx - p_torch| < 3e-7).

## Modes

* `enforce`: the test stops when the classifier says stop. The client gets
  the Stage 1 prediction as the result (`TurboTest` in the final measurement
  and in the summary).
* `shadow`: the test runs to full length. The service records the decision of
  every epsilon on the same traffic. Use this mode to measure accuracy.

## Run

See [the setup guide in the top-level README](../README.md#turbotest-real-time-early-termination)
for the full steps. In short, from the repository root:

```
docker build -t tcp-info-custom tcp-info
docker build -t ndt-fullstack:custom .
docker build -t turbotest-service turbotest
MODEL_DIR=/path/to/model_shared turbotest/deploy/run_stack.sh
(cd ndt7-client && go build -o ndt7-client ./cmd/ndt7-client)
ndt7-client/ndt7-client -server localhost:18080 -scheme ws -upload=false -turbotest enforce -turbotest-epsilon 15
turbotest/deploy/run_stack.sh stop
```

The service without Docker:

```
pip install -r requirements.txt
REDIS_ADDR=127.0.0.1:16379 python -m turbotest.service --model-dir /path/to/model_shared
```

Service options (flag / environment variable):

| Flag | Env | Default | |
|---|---|---|---|
| `--redis-addr` | `REDIS_ADDR` | `localhost:6379` | |
| `--model-dir` | `TURBOTEST_MODEL_DIR` | `/models` | the `model_shared` layout |
| `--epsilon` | `TURBOTEST_EPSILON` | 15 | for jobs without an epsilon |
| `--epsilons` | `TURBOTEST_EPSILONS` | `5,...,35` | classifiers to load |
| `--threshold` | `TURBOTEST_THRESHOLD` | 0.5 | stop if p >= threshold |
| `--poll-ms` | `TURBOTEST_POLL_MS` | 5 | Redis poll period |
| `--max-jobs` | `TURBOTEST_MAX_JOBS` | 5000 | further tests run to full length |
| `--metrics-port` | `TURBOTEST_METRICS_PORT` | 9994 | Prometheus `/metrics` |

ndt-server flags: `-turbotest.redis-addr` (default `$REDIS_ADDR`; empty
disables TurboTest) and `-turbotest.poll-interval` (default 10 ms).

Failure behavior: if Redis or the service is unavailable, or the service
does not decide, the download runs to its normal length. TurboTest can only
shorten a test.

## Tests and evaluation

```
python -m pytest tests                       # unit + integration (needs model_shared, redis-server)
python -m tools.parity_check --model-dir ../../../model_shared
python -m tools.offline_eval --model-dir ../../../model_shared --n 20000 --epsilons 5,10,15,20,25,30,35
python -m tools.live_eval --client /path/to/ndt7-client      # against a running stack
python -m tools.live_report results/live/live_tests.jsonl
```
