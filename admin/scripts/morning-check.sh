#!/bin/bash

echo "===== BROKER STATUS ====="
docker compose ps

echo
echo "===== CLUSTER HEALTH ====="
docker exec -e KAFKA_OPTS="" kafka-1 \
kafka-broker-api-versions \
--bootstrap-server kafka-1:29092 | head -5

echo
echo "===== TOPICS ====="
docker exec -e KAFKA_OPTS="" kafka-1 \
kafka-topics \
--bootstrap-server kafka-1:29092 --list

echo
echo "===== CONSUMER GROUPS ====="
docker exec -e KAFKA_OPTS="" kafka-1 \
kafka-consumer-groups \
--bootstrap-server kafka-1:29092 --list

echo
echo "===== ORDERS ISR CHECK ====="
docker exec -e KAFKA_OPTS="" kafka-1 \
kafka-topics \
--bootstrap-server kafka-1:29092 \
--describe --topic orders | grep Isr
