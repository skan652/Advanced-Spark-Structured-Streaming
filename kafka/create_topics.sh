#!/bin/bash
echo "Creating Kafka topics..."

docker exec -it kafka kafka-topics \
  --create \
  --topic weather \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1

docker exec -it kafka kafka-topics \
  --create \
  --topic invalid-weather \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1

echo "Topics created."