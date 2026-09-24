#!/usr/bin/env bash
# Verifies SML-2 placement on a real Spark standalone cluster, without SGX:
#   - an "enclave" worker that advertises the enclave resource and
#     holds the master key (SOTERIA_FAKE_ENCLAVE=1 stands in for Gramine);
#   - an untrusted worker with neither.
# Then runs soteria.it.SchedulingCheck against it.
#
# Usage: scripts/local-cluster/run.sh   (from the repository root)
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
WORK="$(mktemp -d -t soteria-cluster-XXXX)"
HERE="$ROOT/scripts/local-cluster"
MASTER_PORT="${MASTER_PORT:-7077}"
MASTER="spark://127.0.0.1:$MASTER_PORT"
PIDS=()
cleanup() {
  status=$?
  for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done
  if [[ $status -ne 0 ]]; then
    echo "== failed (exit $status); logs from $WORK:"
    for f in "$WORK"/*.log "$WORK"/*/app-*/*/stderr; do
      [[ -f "$f" ]] && { echo "--- $f"; tail -n 40 "$f"; }
    done
  fi
}
trap cleanup EXIT

JAVA_OPTS=(
  --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED
  --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED
  --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED
  --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED
  --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED
  --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED
  --add-opens=java.base/sun.util.calendar=ALL-UNNAMED -Djdk.reflect.useDirectMethodHandle=false
)

echo "== building jars"
cd "$ROOT"
sbt -batch package Test/package "export Test/fullClasspath" > "$WORK/sbt.log" 2>&1
# `export` prints the classpath as the last plain (non-log) line.
grep -v '^\[' "$WORK/sbt.log" | grep '\.jar' | tail -1 > "$WORK/classpath"
[[ -s "$WORK/classpath" ]] || { echo "could not read the test classpath from sbt"; exit 1; }

# A minimal SPARK_HOME: Workers build executor command lines from $SPARK_HOME/jars.
export SPARK_HOME="$WORK/spark-home"
mkdir -p "$SPARK_HOME/jars"
touch "$SPARK_HOME/RELEASE"
tr ':' '\n' < "$WORK/classpath" | grep '\.jar$' | while read -r j; do ln -sf "$j" "$SPARK_HOME/jars/"; done
cp target/scala-2.12/*.jar "$SPARK_HOME/jars/"
export SPARK_SCALA_VERSION=2.12
CP="$SPARK_HOME/jars/*"

echo "== starting master and workers (logs in $WORK)"
java "${JAVA_OPTS[@]}" -cp "$CP" org.apache.spark.deploy.master.Master \
  --host 127.0.0.1 --port "$MASTER_PORT" --webui-port 0 > "$WORK/master.log" 2>&1 &
PIDS+=($!)
sleep 5

KEY="soteria-master:$(head -c 16 /dev/urandom | base64)"

cat > "$WORK/enclave-worker.conf" <<CONF
spark.worker.resource.enclave.amount 2
spark.worker.resource.enclave.discoveryScript $HERE/enclave-discovery.sh
CONF
SOTERIA_FAKE_ENCLAVE=1 SOTERIA_MASTER_KEYS="$KEY" \
  java "${JAVA_OPTS[@]}" -cp "$CP" org.apache.spark.deploy.worker.Worker \
  --host 127.0.0.1 --cores 2 --memory 2g --webui-port 0 --work-dir "$WORK/enclave-worker" \
  --properties-file "$WORK/enclave-worker.conf" "$MASTER" > "$WORK/enclave-worker.log" 2>&1 &
PIDS+=($!)

env -u SOTERIA_MASTER_KEYS -u SOTERIA_MASTER_KEYS_FILE \
  java "${JAVA_OPTS[@]}" -cp "$CP" org.apache.spark.deploy.worker.Worker \
  --host 127.0.0.1 --cores 2 --memory 2g --webui-port 0 --work-dir "$WORK/untrusted-worker" \
  "$MASTER" > "$WORK/untrusted-worker.log" 2>&1 &
PIDS+=($!)
sleep 8

echo "== running SchedulingCheck"
SOTERIA_MASTER_KEYS="$KEY" java "${JAVA_OPTS[@]}" -cp "$CP" \
  -Dspark.master="$MASTER" \
  -Dspark.driver.host=127.0.0.1 \
  -Dspark.ui.enabled=false \
  -Dspark.executor.cores=1 \
  -Dspark.executor.memory=512m \
  -Dspark.executor.resource.enclave.amount=1 \
  -Dspark.task.resource.enclave.amount=1 \
  -Dspark.dynamicAllocation.enabled=true \
  -Dspark.dynamicAllocation.shuffleTracking.enabled=true \
  -Dspark.dynamicAllocation.minExecutors=2 \
  -Dspark.dynamicAllocation.maxExecutors=2 \
  -Dspark.soteria.untrusted.executor.memory=512m \
  "-Dspark.executor.extraJavaOptions=${JAVA_OPTS[*]}" \
  soteria.it.SchedulingCheck "$WORK/data" 2>"$WORK/driver.log"
