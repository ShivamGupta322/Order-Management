#!/usr/bin/env sh
# Creates the four order topics. Safe to re-run: existing topics are left alone.
set -eu

# Git Bash on Windows rewrites /opt/... into a Windows path before docker sees it.
export MSYS_NO_PATHCONV=1

PARTITIONS="${PARTITIONS:-3}"

for topic in orders orders.retry orders.completed orders.dlq; do
  docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --create --if-not-exists \
    --topic "$topic" \
    --partitions "$PARTITIONS" \
    --replication-factor 1
done

docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list
