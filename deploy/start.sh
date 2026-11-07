#!/usr/bin/env bash
# Runs Redpanda, the producer and the consumer in one container.
set -u

rpk redpanda start \
  --smp 1 --memory 1G --overprovisioned --mode dev-container \
  --kafka-addr 0.0.0.0:9092 --advertise-kafka-addr localhost:9092 &

until rpk cluster health --exit-when-healthy >/dev/null 2>&1; do sleep 1; done
rpk topic create ticks -p 6 >/dev/null 2>&1 || true
echo "redpanda ready"

# The producer exits on WebSocket errors, so restart it with a short backoff.
(
  while true; do
    /app/producer
    echo "producer exited ($?), restarting in 5s"
    sleep 5
  done
) &

cd /app
exec /app/consumer
