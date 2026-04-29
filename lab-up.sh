#!/bin/bash
set -e

cd /workspaces/kafka-lab

echo "▶ Starting Kafka lab..."

# Try start first; if containers don't exist, fall back to 'up -d'
if ! docker compose start 2>/dev/null; then
  echo "  Containers not found — running 'docker compose up -d' instead..."
  docker compose up -d
fi

echo ""
echo "▶ Waiting for cluster to settle..."
sleep 8

echo ""
echo "▶ Container status:"
docker compose ps

echo ""
echo "▶ Verifying broker connectivity..."
docker exec -e KAFKA_OPTS="" kafka-1 kafka-broker-api-versions \
  --bootstrap-server kafka-1:29092 2>/dev/null | head -1 \
  && echo "  ✓ Broker 1 responding" \
  || echo "  ✗ Broker 1 not ready — give it 30s and retry"

echo ""
echo "▶ Topics in cluster:"
docker exec -e KAFKA_OPTS="" kafka-1 kafka-topics \
  --bootstrap-server kafka-1:29092 --list 2>/dev/null \
  || echo "  (cluster still starting)"

echo ""
echo "Lab ready."
echo "  CLI:        kx kafka-topics --bootstrap-server kafka-1:29092 --list"
echo "  Web UIs:    Click PORTS tab in VS Code → globe icon next to ports 8080/9090/3000"
