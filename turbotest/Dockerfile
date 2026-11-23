# TurboTest real-time inference service.
#   docker build -t turbotest-service turbotest/
#   docker run --network ndt-net -e REDIS_ADDR=redis:6379 \
#     -v /path/to/model_shared:/models:ro turbotest-service
FROM python:3.10-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY turbotest ./turbotest
ENV TURBOTEST_MODEL_DIR=/models \
    TURBOTEST_ONNX_CACHE=/cache \
    PYTHONUNBUFFERED=1
RUN mkdir -p /cache
ENTRYPOINT ["python", "-m", "turbotest.service"]
