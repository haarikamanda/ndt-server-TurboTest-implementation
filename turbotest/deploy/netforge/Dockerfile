# NetForge (netreplica solo) bottleneck + ndt7 client, for TurboTest tests.
# Build context: the NetForge repo's src/ directory, e.g.
#   docker build -t turbotest-netforge -f turbotest/deploy/netforge/Dockerfile /path/to/netforge/src
# The NetForge image lacks tcpreplay, which its CTP (cross-traffic) replay uses.
FROM ubuntu:22.04
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
      python3 iproute2 iptables iputils-ping tcpreplay ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /workspace
COPY setup.sh cleanup.sh controler.py /workspace/
RUN chmod +x setup.sh cleanup.sh
ENTRYPOINT ["sleep", "infinity"]
