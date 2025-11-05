# Redis Client for NDT Server

`/ndt-server/redis` creates a wrapper for the public redis package to add methods and functionality (type safety, naming convention, error handling) specific to early termination.

## Testing
There are internal tests for the redis client implementation in `ndt-server/redis` and tests for earlyTermination in `ndt-server/ndt7/handler`.

The tests require a running Redis instance and will gracefully skip if Redis is not available.

If the redis server is not up already, start the redis image using docker: `docker run -d -p 6379:6379 redis:latest`

To run internal tests: `go test ./redis -v -run Test_SetAndGetTerminationFlag`

To run early termination tests: `go test ./ndt7/handler -v -run Test_checkEarlyTermination`

The functions in hander_test.go should be called as a goroutine during a measurement:
```
ctx, cancel := context.WithCancel(parentCtx)
go h.checkEarlyTermination(ctx, uuid, cancel)
// When flag is set to 1 in Redis, cancel() will be called
```

## Running the NDT Server for Development
Alternatively, you can launch the redis server along with the fullstack ndt-server using docker-compose.

**Prerequisites:**
- Docker and docker-compose installed
- Might need to tag `git tag v0.0.x-dev` to avoid git issues
- Build the ndt-server image first (rebuild after for any changes to be reflected in the binary): `docker build -t ndt-server -f Dockerfile.local .`
- Generate certificate: `docker-compose run ndt-server ./gen_local_test_certs.bash`
- Ensure local directories exist: `./certs, ./html, ./resultsdir, ./localgcs`
    - If they don't exist: `install -d /certs /datadir /html /resultsdir /localgcs`

**Run the full-stack NDT server: `docker-compose up -d`**
  - This will start all services defined in the compose file:
	  - ndt-server (main NDT7 server)
	  - generate-schemas (runs first, then exits)
	  - jostler (data bundling/archiving) *Note that the jostler image is only compatible with Linux
	  - redis (Redis server)
- Clients connect to port 4443 (HTTPS) or 8080 (HTTP) to use ndt7.

