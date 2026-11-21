#!/bin/bash
set -euo pipefail
kafka-topics --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 \
  --create --if-not-exists --topic movie-events --partitions 3 --replication-factor 3 \
  --config min.insync.replicas=2
kafka-configs --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 \
  --entity-type topics --entity-name movie-events --alter --add-config min.insync.replicas=2
kafka-topics --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 \
  --describe --topic movie-events
