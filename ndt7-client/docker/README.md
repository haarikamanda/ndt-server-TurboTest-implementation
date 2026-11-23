# ndt7-client Docker Usage

This directory contains the files needed to run the single-shot `ndt7-client` inside a container with persistent logs.

## Build the image

```bash
docker build -f Dockerfile.client -t ndt7-client ..
```

Run the command from the repository root so `Dockerfile.client` can access the Go module files.

## Runtime environment variables

- `EXPERIMENT`: Experiment/metadata label. Automatically passed to `--extra_metadata` and used for the logfile name (`ndt7_raw_${EXPERIMENT}.log`). Defaults to `default`.
- `NDT7_SERVER`: Host:port passed to `--server`. Defaults to `127.0.0.1:18080`.
- `NDT7_SCHEME`: Protocol passed to `--scheme` (`ws` or `wss`). Defaults to `ws`.
- `LOG_DIR`: Directory inside the container that collects log files. Defaults to `/logs` (declared as a volume).

Additional CLI arguments may be appended after the image name and override any defaults.

## Sample run

```bash
mkdir -p ./ndt7-logs
docker run --rm \
  -e EXPERIMENT=haarikatest2 \
  -e NDT7_SERVER=128.111.5.231:18080 \
  -e NDT7_SCHEME=ws \
  -v $(pwd)/ndt7-logs:/logs \
  ndt7-client --no-verify
```
- SERVER IP can be changed depending on the configuration. 
- `--no-verify` is forwarded directly to the binary.
- The entrypoint injects `--scheme $NDT7_SCHEME`, `--server $NDT7_SERVER`, and `--extra_metadata $EXPERIMENT` when those flags are omitted.
- All stdout/stderr from the test is appended to `/logs/ndt7_raw_${EXPERIMENT}.log`, which maps to the host mount.
