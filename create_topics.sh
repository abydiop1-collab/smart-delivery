#!/bin/bash
# Étape A : Création du topic Kafka avec 3 partitions

echo "==> Création du topic 'delivery-locations' avec 3 partitions..."
docker exec kafka kafka-topics --create \
  --topic delivery-locations \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1

echo "==> Création du topic 'urgent-maintenance'..."
docker exec kafka kafka-topics --create \
  --topic urgent-maintenance \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1

echo "==> Topics créés avec succès !"
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092
