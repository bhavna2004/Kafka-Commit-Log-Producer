#!/usr/bin/env bash
set -euo pipefail
export MSYS_NO_PATHCONV=1

echo "===================================================="
echo " Kafka Data Replication "
echo "===================================================="

if command -v docker-compose >/dev/null 2>&1; then
  COMPOSE="docker-compose"
elif docker compose version >/dev/null 2>&1; then
  COMPOSE="docker compose"
else
  echo "ERROR: Docker Compose is not available"
  exit 1
fi

command -v docker >/dev/null 2>&1 || {
  echo "ERROR: Docker is not installed or not on PATH"
  exit 1
}

docker info >/dev/null 2>&1 || {
  echo "ERROR: Docker daemon is not running"
  exit 1
}


echo ""
echo "===================================================="
echo " BUILDING AND PUSHING DOCKER IMAGES"
echo "===================================================="

echo "[BUILD] Building commit-log-producer image..."
docker build \
  -t bhavna2004/commit-log-producer:latest \
  -f Dockerfile \
  .

echo "[BUILD] Building enhanced-mirrormaker image..."
docker build \
  -t bhavna2004/enhanced-mirrormaker2:latest \
  -f ../kafka/Dockerfile \
  ../kafka

echo "[PUSH] Pushing commit-log-producer to Docker Hub..."
docker push bhavna2004/commit-log-producer:latest

echo "[PUSH] Pushing enhanced-mirrormaker to Docker Hub..."
docker push bhavna2004/enhanced-mirrormaker2:latest

echo "✅ Images built and pushed successfully"

echo ""
echo "[STEP 1] Cleaning any existing Docker/Kafka state..."
$COMPOSE down -v --remove-orphans || true
docker network prune -f || true

echo ""
echo "[STEP 2] Starting Kafka clusters (primary + standby)..."
$COMPOSE up -d primary-kafka standby-kafka

echo "[INFO] Waiting for Kafka brokers to be healthy..."

until docker exec primary-kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server primary-kafka:9092 \
  --list >/dev/null 2>&1; do
  echo "  Waiting for primary-kafka..."
  sleep 5
done

echo "✅ primary-kafka is ready"

until docker exec standby-kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server standby-kafka:9092 \
  --list >/dev/null 2>&1; do
  echo "  Waiting for standby-kafka..."
  sleep 5
done
echo "✅ standby-kafka is ready"

echo ""
echo "[STEP 3] Creating commit-log topic BEFORE MM2 starts..."
docker exec primary-kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server primary-kafka:9092 \
  --create --if-not-exists \
  --topic commit-log \
  --partitions 1 \
  --replication-factor 1 \
  --config retention.ms=60000

echo ""
echo "[STEP 4] Starting MirrorMaker 2..."
$COMPOSE up -d mirrormaker

echo "[INFO] Waiting for MirrorSourceTask to initialize..."
ATTEMPTS=0
MAX_ATTEMPTS=24
while true; do
  MM2_INIT_LOGS=$(docker logs mirrormaker 2>&1 || true)
  if echo "$MM2_INIT_LOGS" | grep -q "replicating.*topic-partitions"; then
    echo "[INFO] MM2 successfully assigned commit-log for replication"
    break
  fi
  ATTEMPTS=$((ATTEMPTS + 1))
  if [ "$ATTEMPTS" -ge "$MAX_ATTEMPTS" ]; then
    echo "[WARN] MM2 may not have picked up commit-log — check: docker logs mirrormaker"
    break
  fi
  echo "  Waiting for MM2 to initialize... ($ATTEMPTS/$MAX_ATTEMPTS)"
  sleep 5
done

echo ""
echo "===================================================="
echo " SCENARIO 1: Normal Replication Flow"
echo "===================================================="

echo "[SCENARIO 1] Producing 1000 messages to primary..."
$COMPOSE run --rm producer \
  --count 1000 \
  --bootstrap-server primary-kafka:9092

echo "[SCENARIO 1] Verifying replication on standby..."
PASSED=false
for i in {1..24}; do
  MSG_COUNT=$(docker exec standby-kafka \
    /opt/kafka/bin/kafka-get-offsets.sh \
    --bootstrap-server standby-kafka:9092 \
    --topic primary.commit-log \
    --time -1 2>/dev/null | awk -F: '{print $3}' || true)

  if [ -n "$MSG_COUNT" ] && [ "$MSG_COUNT" -gt 900 ] 2>/dev/null; then
    echo "  primary.commit-log found on standby (messages: $MSG_COUNT)"
    echo "✅ SCENARIO 1 PASSED — Replication verified ($MSG_COUNT messages)"
    PASSED=true
    break
  fi

  echo "  Waiting for replication... ($i/24) standby offset: ${MSG_COUNT:-0}"
  sleep 5
done

if [ "$PASSED" = false ]; then
  echo "❌ SCENARIO 1 FAILED — Less than 900 messages replicated to standby"
  echo "--- MM2 logs ---"
  docker logs mirrormaker 2>&1 | tail -40
  exit 1
fi

echo ""
echo "===================================================="
echo " SCENARIO 2: Log Truncation Detection (Fail-Fast)"
echo "===================================================="

echo "[SCENARIO 2] Pausing MM2..."
$COMPOSE stop mirrormaker
sleep 5

echo "[SCENARIO 2] Applying aggressive retention on commit-log..."
docker exec primary-kafka \
  /opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server primary-kafka:9092 \
  --entity-type topics \
  --entity-name commit-log \
  --alter \
  --add-config retention.ms=2000,segment.bytes=1048576

echo "[SCENARIO 2] Producing 10000 messages while MM2 is paused..."
$COMPOSE run --rm producer \
  --count 10000 \
  --bootstrap-server primary-kafka:9092

echo "[SCENARIO 2] Waiting for truncation to occur..."
ATTEMPTS=0
MAX_ATTEMPTS=20
while true; do
  OFFSET=$(docker exec primary-kafka \
    /opt/kafka/bin/kafka-get-offsets.sh \
    --bootstrap-server primary-kafka:9092 \
    --topic commit-log \
    --time -2 2>/dev/null | awk -F: '{print $3}' || true)

  echo "  Current logStartOffset: $OFFSET"

  if [ -n "$OFFSET" ] && [ "$OFFSET" -gt 1000 ]; then
    echo "✅ Truncation confirmed — logStartOffset advanced to $OFFSET"
    break
  fi

  ATTEMPTS=$((ATTEMPTS + 1))
  if [ "$ATTEMPTS" -ge "$MAX_ATTEMPTS" ]; then
    echo "❌ Truncation did not happen — exiting"
    exit 1
  fi

  echo "  Waiting... ($ATTEMPTS/$MAX_ATTEMPTS)"
  sleep 5
done

echo "[SCENARIO 2] Restoring default retention..."
docker exec primary-kafka \
  /opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server primary-kafka:9092 \
  --entity-type topics \
  --entity-name commit-log \
  --alter \
  --delete-config retention.ms,segment.bytes

echo "[SCENARIO 2] Producing 50 new messages at post-truncation offsets..."
$COMPOSE run --rm producer \
  --count 50 \
  --bootstrap-server primary-kafka:9092

echo "[SCENARIO 2] Restarting MM2 — expecting fail-fast on data loss..."
$COMPOSE start mirrormaker

TRUNCATION_DETECTED=false
echo "[INFO] Waiting for MM2 to detect truncation..."
ATTEMPTS=0
MAX_ATTEMPTS=24
while true; do
  MM2_LOGS=$(docker logs mirrormaker 2>&1 || true)
  if echo "$MM2_LOGS" | grep -qE "Log truncation detected|DataLossException|OFFSET GAP DETECTED"; then
    echo "✅ Truncation detected — breaking wait loop"
    TRUNCATION_DETECTED=true
    break
  fi
  ATTEMPTS=$((ATTEMPTS + 1))
  if [ "$ATTEMPTS" -ge "$MAX_ATTEMPTS" ]; then
    echo "[INFO] Max wait reached — proceeding to check logs"
    break
  fi
  echo "  Waiting for truncation detection... ($ATTEMPTS/$MAX_ATTEMPTS)"
  sleep 5
done

if [ "$TRUNCATION_DETECTED" = true ]; then
  echo "✅ SCENARIO 2 PASSED — MM2 detected log truncation and failed fast"
  docker logs mirrormaker 2>&1 | \
    grep -E "Log truncation detected|DataLossException|OFFSET GAP DETECTED" | tail -3
else
  echo "❌ SCENARIO 2 FAILED — No truncation detection found in MM2 logs"
  echo "--- MM2 logs ---"
  docker logs mirrormaker 2>&1 | tail -60
  exit 1
fi

echo ""
echo "===================================================="
echo " SCENARIO 3: Topic Reset Recovery"
echo "===================================================="

echo "[SCENARIO 3] Full reset — clean state for scenario..."
$COMPOSE down -v --remove-orphans
docker volume prune -f || true
$COMPOSE up -d primary-kafka standby-kafka

echo "[INFO] Waiting for Kafka brokers to be healthy..."
ATTEMPTS=0
MAX_ATTEMPTS=12
while true; do
  if docker exec primary-kafka /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server primary-kafka:9092 --list > /dev/null 2>&1; then
    echo "[INFO] primary-kafka is ready"
    break
  fi
  ATTEMPTS=$((ATTEMPTS + 1))
  if [ "$ATTEMPTS" -ge "$MAX_ATTEMPTS" ]; then
    echo "[WARN] primary-kafka not ready after max wait"
    break
  fi
  echo "  Waiting for primary-kafka... ($ATTEMPTS/$MAX_ATTEMPTS)"
  sleep 5
done

echo "[SCENARIO 3] Creating commit-log topic before MM2 starts..."
docker exec primary-kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server primary-kafka:9092 \
  --create --if-not-exists \
  --topic commit-log \
  --partitions 1 \
  --replication-factor 1 \
  --config retention.ms=60000

echo "[SCENARIO 3] Starting MM2 with clean state..."
$COMPOSE up -d mirrormaker

echo "[INFO] Waiting for MirrorSourceTask to initialize..."
ATTEMPTS=0
MAX_ATTEMPTS=24
while true; do
  MM2_LOGS=$(docker logs mirrormaker 2>&1 || true)
  if echo "$MM2_LOGS" | grep -q "replicating.*topic-partitions"; then
    echo "[INFO] MirrorSourceTask is running and replicating"
    break
  fi
  ATTEMPTS=$((ATTEMPTS + 1))
  if [ "$ATTEMPTS" -ge "$MAX_ATTEMPTS" ]; then
    echo "[WARN] MirrorSourceTask not confirmed — check logs"
    docker logs mirrormaker 2>&1 | tail -20
    break
  fi
  echo "  Waiting for MirrorSourceTask... ($ATTEMPTS/$MAX_ATTEMPTS)"
  sleep 5
done

echo "[SCENARIO 3] Producing 100 messages so MM2 builds a committed offset..."
$COMPOSE run --rm producer \
  --count 100 \
  --bootstrap-server primary-kafka:9092

echo "[SCENARIO 3] Waiting for MM2 to replicate and commit offsets..."
ATTEMPTS=0
MAX_ATTEMPTS=24
while true; do
  STANDBY_OFFSET=$(docker exec standby-kafka \
    /opt/kafka/bin/kafka-get-offsets.sh \
    --bootstrap-server standby-kafka:9092 \
    --topic primary.commit-log \
    --time -1 2>/dev/null | awk -F: '{print $3}' || true)

  if [ -n "$STANDBY_OFFSET" ] && [ "$STANDBY_OFFSET" -gt 0 ] 2>/dev/null; then
    echo "  Replication confirmed — standby offset: $STANDBY_OFFSET"
    break
  fi

  ATTEMPTS=$((ATTEMPTS + 1))
  if [ "$ATTEMPTS" -ge "$MAX_ATTEMPTS" ]; then
    echo "[WARN] Replication not confirmed after max wait — proceeding anyway"
    break
  fi

  echo "  Waiting for replication to commit... ($ATTEMPTS/$MAX_ATTEMPTS)"
  sleep 5
done

STANDBY_CHECK=$(docker exec standby-kafka \
  /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server standby-kafka:9092 \
  --topic primary.commit-log \
  --time -1 2>/dev/null || true)
echo "[SCENARIO 3] Standby state before reset: $STANDBY_CHECK"

echo "[SCENARIO 3] Stopping MM2 before topic deletion..."
$COMPOSE stop mirrormaker
sleep 5

echo "[SCENARIO 3] Deleting commit-log topic..."
docker exec primary-kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server primary-kafka:9092 \
  --delete --topic commit-log || true

echo "[SCENARIO 3] Waiting for topic deletion to complete..."
ATTEMPTS=0
MAX_ATTEMPTS=36
while true; do
  TOPIC_LIST=$(docker exec primary-kafka \
    /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server primary-kafka:9092 \
    --list 2>/dev/null || true)
  if ! echo "$TOPIC_LIST" | grep -q "^commit-log$";
    then
      echo "✅ commit-log fully removed from Kafka metadata"
      sleep 5
      break
    fi

  ATTEMPTS=$((ATTEMPTS + 1))
  if [ "$ATTEMPTS" -ge "$MAX_ATTEMPTS" ]; then
    echo "❌ Topic deletion failed — exiting"
    exit 1
  fi

  echo "  Waiting for topic deletion... ($ATTEMPTS/$MAX_ATTEMPTS)"
  sleep 5
done

echo "[SCENARIO 3] Waiting for Kafka to fully release topic name..."

ATTEMPTS=0
MAX_ATTEMPTS=20


while true; do
  if docker exec primary-kafka \
    /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server primary-kafka:9092 \
    --list | grep -q "^commit-log$"; then
    echo "  Still exists internally..."
  else
    echo "✅ Kafka fully released topic name"
    break
  fi

  ATTEMPTS=$((ATTEMPTS + 1))
  if [ "$ATTEMPTS" -ge "$MAX_ATTEMPTS" ]; then
    echo "❌ Kafka did not release topic name — exiting"
    exit 1
  fi

  echo "  Waiting for Kafka internal cleanup... ($ATTEMPTS/$MAX_ATTEMPTS)"
  sleep 3
done

echo "[SCENARIO 3] Starting MM2 after topic deletion..."
$COMPOSE start mirrormaker

echo "[SCENARIO 3] Allowing Kafka metadata to stabilize..."
sleep 10

echo "[SCENARIO 3] Recreating commit-log topic..."

docker exec primary-kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server primary-kafka:9092 \
  --create \
  --if-not-exists \
  --topic commit-log \
  --partitions 1 \
  --replication-factor 1 \
  --config retention.ms=60000 || true

echo "[SCENARIO 3] Producing 50 fresh messages to the recreated topic..."
$COMPOSE run --rm producer \
  --count 50 \
  --bootstrap-server primary-kafka:9092

RESET_DETECTED=false
echo "[INFO] Waiting for MM2 to detect topic reset..."
ATTEMPTS=0
MAX_ATTEMPTS=24
while true; do
  MM2_LOGS=$(docker logs mirrormaker 2>&1 || true)
  if echo "$MM2_LOGS" | grep -q "Topic reset detected"; then
    echo "✅ Topic reset detected — breaking wait loop"
    RESET_DETECTED=true
    break
  fi
  ATTEMPTS=$((ATTEMPTS + 1))
  if [ "$ATTEMPTS" -ge "$MAX_ATTEMPTS" ]; then
    echo "[INFO] Max wait reached — proceeding to check logs"
    break
  fi
  echo "  Waiting for topic reset detection... ($ATTEMPTS/$MAX_ATTEMPTS)"
  sleep 5
done

if [ "$RESET_DETECTED" = true ]; then
  echo "✅ SCENARIO 3 PASSED — MM2 detected topic reset and recovered automatically"
  docker logs mirrormaker 2>&1 | grep "Topic reset detected" | tail -3
else
  echo "❌ SCENARIO 3 FAILED — Topic reset detection not found in MM2 logs"
  echo "--- MM2 logs ---"
  docker logs mirrormaker 2>&1 | tail -60
  exit 1
fi

echo ""
echo "===================================================="
echo " FINAL EVENT SUMMARY FROM MM2 LOGS"
echo "===================================================="
docker logs mirrormaker 2>&1 | grep -E \
  "Log truncation detected|DataLossException|OFFSET GAP DETECTED|Topic reset detected" || true

echo ""
echo "DEMONSTRATED CAPABILITIES:"
echo "✔ Real-time replication"
echo "✔ Fail-fast data loss detection"
echo "✔ Topic reset auto-recovery"
echo ""
echo "ALL SCENARIOS COMPLETED SUCCESSFULLY"