# Runs the SOTERIA Spark JVM under Gramine.
#   make <target>          gramine-direct (no SGX needed; checks the setup)
#   make SGX=1 <target>    gramine-sgx (signed enclave)
# See README.md for the bring-up order.

SHELL        := /bin/bash
ROOT         := $(abspath ..)
JAVA_HOME    ?= /usr/lib/jvm/java-17-openjdk
SPARK_HOME   ?= /opt/spark
WORK_DIR     ?= $(CURDIR)/work
SOTERIA_DIR  ?= $(CURDIR)/dist
ARCH_LIBDIR  ?= $(if $(wildcard /lib/x86_64-linux-gnu/libc.so.6),/lib/x86_64-linux-gnu,/lib64)
DISCOVERY    ?= $(ROOT)/scripts/local-cluster/enclave-discovery.sh

SGX          ?= 0
DEBUG        ?= 0
ENCLAVE_SIZE ?= 8G
MAX_THREADS  ?= 256
EDMM         ?= 0
USE_EXINFO   ?= $(EDMM)
JVM_HEAP     ?= 2g

MASTER_HOST  ?= 127.0.0.1
MASTER_PORT  ?= 7077
MASTER       := spark://$(MASTER_HOST):$(MASTER_PORT)
WORKER_CORES ?= 2
WORKER_MEM   ?= 4g

# Symlinks are resolved: trusted files must match the paths the JVM opens.
JAVA_REAL    := $(realpath $(JAVA_HOME))
SPARK_REAL   := $(realpath $(SPARK_HOME))
JAVA_CONF    := $(realpath $(JAVA_REAL)/conf)
CRYPTO_JAVA  := $(realpath /etc/crypto-policies/back-ends/java.config)
bool          = $(if $(filter 1 true,$(1)),true,false)

ifeq ($(SGX),1)
GRAMINE      := gramine-sgx
SGX_FILES    := java.manifest.sgx java.sig
else
GRAMINE      := gramine-direct
SGX_FILES    :=
endif
LOG_LEVEL    := $(if $(filter 1,$(DEBUG)),debug,error)

# The same module openings as build.sbt; serial GC and no container probing
# keep the JVM's memory layout predictable inside the enclave.
# Heap flags are kept apart: Spark rejects -Xmx in spark.executor.extraJavaOptions
# (executor heaps come from spark.executor.memory).
JVM_HEAP_OPTS := -Xms256m -Xmx$(JVM_HEAP)
JVM_BASE_OPTS := -Dlog4j.configurationFile=$(CURDIR)/log4j2.properties \
  -XX:+UseSerialGC -XX:-UseContainerSupport -XX:-UsePerfData \
  -XX:ReservedCodeCacheSize=64m -XX:CompressedClassSpaceSize=64m -XX:MaxMetaspaceSize=256m \
  -XX:+PreserveFramePointer \
  --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
  --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED \
  --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED \
  --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
  --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
  --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED \
  --add-opens=java.base/sun.util.calendar=ALL-UNNAMED -Djdk.reflect.useDirectMethodHandle=false
JVM_OPTS := $(JVM_HEAP_OPTS) $(JVM_BASE_OPTS)

SPARK_CP     := $(SPARK_REAL)/jars/*:$(SOTERIA_DIR)/*

.PHONY: all check-env dist hello spark-version start-master start-worker-enclave start-worker-native run-check bench bench-vanilla bench-plot clean

all: java.manifest $(SGX_FILES)

check-env:
	@test -x "$(JAVA_REAL)/bin/java" || { echo "JAVA_HOME=$(JAVA_HOME) has no bin/java"; exit 1; }
	@test -d "$(SPARK_REAL)/jars" || { echo "SPARK_HOME=$(SPARK_HOME) not found (scripts/install_spark.sh)"; exit 1; }
	@command -v $(GRAMINE) >/dev/null || { echo "$(GRAMINE) not found (scripts/install_gramine.sh)"; exit 1; }

# SOTERIA jars built by sbt (sbt package Test/package), copied so that only
# they, not the whole target/ tree, are measured into the enclave.
# Always rebuilds (incremental, fast when nothing changed) so a git pull is
# never run with stale jars.
dist:
	cd $(ROOT) && sbt -batch package Test/package
	mkdir -p $(SOTERIA_DIR)
	cp $(ROOT)/target/scala-2.12/soteria_2.12-*.jar $(SOTERIA_DIR)/

hello/HelloWorld.class: hello/HelloWorld.java
	$(JAVA_REAL)/bin/javac --release 17 -d hello $<

java.manifest: java.manifest.template dist hello/HelloWorld.class check-env
	mkdir -p $(WORK_DIR)
	gramine-manifest \
	  -Dlog_level=$(LOG_LEVEL) \
	  -Darch_libdir=$(ARCH_LIBDIR) \
	  -Djava_home=$(JAVA_REAL) \
	  -Djava_conf=$(JAVA_CONF) \
	  -Dcrypto_java=$(CRYPTO_JAVA) \
	  -Dspark_home=$(SPARK_REAL) \
	  -Dsoteria_dir=$(SOTERIA_DIR) \
	  -Dwork_dir=$(WORK_DIR) \
	  -Dhere=$(CURDIR) \
	  -Ddebug=$(call bool,$(DEBUG)) \
	  -Dedmm=$(call bool,$(EDMM)) \
	  -Duse_exinfo=$(call bool,$(USE_EXINFO)) \
	  -Denclave_size=$(ENCLAVE_SIZE) \
	  -Dmax_threads=$(MAX_THREADS) \
	  -Dremote_attestation=none \
	  $< > $@

java.manifest.sgx java.sig &: java.manifest
	gramine-sgx-sign --manifest $< --output java.manifest.sgx
	gramine-sgx-sigstruct-view java.sig

# Step 1: a trivial JVM program.
hello: all
	$(GRAMINE) java $(JVM_OPTS) -cp $(CURDIR)/hello HelloWorld

# Step 2: load Spark's classes.
spark-version: all
	$(GRAMINE) java $(JVM_OPTS) -cp '$(SPARK_CP)' org.apache.spark.deploy.SparkSubmit --version

# Step 3: cluster. The Master only schedules and sees no data, so it runs natively.
start-master: check-env
	SPARK_LOCAL_IP=$(MASTER_HOST) $(JAVA_REAL)/bin/java $(JVM_OPTS) -cp '$(SPARK_CP)' org.apache.spark.deploy.master.Master \
	  --host $(MASTER_HOST) --port $(MASTER_PORT) --webui-port 8080

# The enclave Worker daemon runs natively: it only launches executors and
# never handles data. Its JAVA_HOME points at a wrapper, so every executor it
# launches is a fresh Gramine process (an enclave with SGX=1). Starting each
# executor from the host avoids forking a multi-GB JVM inside Gramine.
ENCLAVE_JDK := $(WORK_DIR)/enclave-jdk

# INSECURE (bring-up only): the dataset master key lives in a host file that
# the driver and the enclave executors read. Replaced by RA-TLS provisioning.
KEYS_FILE := $(WORK_DIR)/master.keys

$(KEYS_FILE):
	mkdir -p $(@D)
	umask 077 && printf 'soteria-master:%s\n' "$$(head -c 16 /dev/urandom | base64)" > $@
	@echo "created bring-up master key file $@"

$(ENCLAVE_JDK)/bin/java: Makefile
	mkdir -p $(@D)
	printf '#!/bin/sh\n# Generated by gramine/Makefile: runs a Spark executor under %s.\nexec %s %s/java "$$@"\n' \
	  $(GRAMINE) $(GRAMINE) $(CURDIR) > $@
	chmod +x $@

start-worker-enclave: all $(ENCLAVE_JDK)/bin/java $(KEYS_FILE)
	printf 'spark.worker.resource.enclave.amount %s\nspark.worker.resource.enclave.discoveryScript %s\n' \
	  $(WORKER_CORES) $(DISCOVERY) > $(WORK_DIR)/enclave-worker.conf
	JAVA_HOME=$(ENCLAVE_JDK) SPARK_HOME=$(SPARK_REAL) SPARK_SCALA_VERSION=2.12 SPARK_LOCAL_IP=$(MASTER_HOST) SOTERIA_ENCLAVE_SLOTS=$(WORKER_CORES) \
	  SOTERIA_MASTER_KEYS_FILE=$(KEYS_FILE) \
	  $(if $(filter 1,$(SGX)),SOTERIA_ENCLAVE_RUNNER=gramine-sgx,SOTERIA_FAKE_ENCLAVE=1) \
	  $(JAVA_REAL)/bin/java $(JVM_OPTS) -cp '$(SPARK_CP)' org.apache.spark.deploy.worker.Worker \
	  --host $(MASTER_HOST) --cores $(WORKER_CORES) --memory $(WORKER_MEM) \
	  --webui-port 0 --work-dir $(WORK_DIR)/enclave-worker --properties-file $(WORK_DIR)/enclave-worker.conf $(MASTER)

# Untrusted worker: native, no enclave resource, no keys.
start-worker-native: check-env
	env -u SOTERIA_MASTER_KEYS -u SOTERIA_MASTER_KEYS_FILE -u SOTERIA_FAKE_ENCLAVE -u SOTERIA_ENCLAVE_RUNNER \
	  SPARK_HOME=$(SPARK_REAL) SPARK_SCALA_VERSION=2.12 SPARK_LOCAL_IP=$(MASTER_HOST) $(JAVA_REAL)/bin/java $(JVM_OPTS) -cp '$(SPARK_CP)' org.apache.spark.deploy.worker.Worker \
	  --host $(MASTER_HOST) --cores $(WORKER_CORES) --memory $(WORKER_MEM) \
	  --webui-port 0 --work-dir $(WORK_DIR)/native-worker $(MASTER)

# Driver settings for the cluster started above: executors of the default
# profile need the enclave resource; SML-2 adds untrusted executors on demand.
CLUSTER_CONF := -Dspark.master=$(MASTER) -Dspark.driver.host=$(MASTER_HOST) -Dspark.ui.enabled=false \
  -Dspark.executor.cores=1 -Dspark.executor.memory=1g \
  -Dspark.executor.resource.enclave.amount=1 -Dspark.task.resource.enclave.amount=1 \
  -Dspark.dynamicAllocation.enabled=true -Dspark.dynamicAllocation.shuffleTracking.enabled=true \
  -Dspark.dynamicAllocation.minExecutors=$(WORKER_CORES) -Dspark.dynamicAllocation.maxExecutors=$(WORKER_CORES) \
  -Dspark.soteria.untrusted.executor.memory=1g \
  -Dspark.executor.extraJavaOptions='$(JVM_BASE_OPTS)' \
  -Dspark.executor.extraClassPath='$(SOTERIA_DIR)/*'

# Lines of a JVM log that matter; the rest (child-process fallbacks inside
# Gramine: "rm", error=13) is known noise.
summary = grep -E "^(executors|tasks|LR training|PASS|FAIL|DONE|ERROR:)|Exception in thread|Caused by| ERROR " $(1) \
  | grep -vE '"rm"|error=13' | sed -E 's/^[0-9\/]+ [0-9:]+ //' | awk '!seen[$$0]++' | head -20

# Step 4: the SML-2 placement check, with the driver in an enclave.
# The full log goes to work/run-check.log; a summary is printed at the end.
run-check: all $(KEYS_FILE)
	rm -rf $(WORK_DIR)/data
	set -o pipefail; SPARK_LOCAL_IP=$(MASTER_HOST) SOTERIA_MASTER_KEYS_FILE=$(KEYS_FILE) $(GRAMINE) java $(JVM_OPTS) -cp '$(SPARK_CP)' \
	  $(CLUSTER_CONF) soteria.it.SchedulingCheck $(WORK_DIR)/data 2>&1 | tee $(WORK_DIR)/run-check.log; \
	rc=$$?; \
	echo "==== run-check summary (full log: $(WORK_DIR)/run-check.log) ===="; \
	$(call summary,$(WORK_DIR)/run-check.log); \
	exit $$rc

# Phase 4: benchmarks (see ../bench/README.md). Results are appended to
# work/bench/results.csv; datasets are generated once per scale and kept.
ALGO        ?= all
MODE        ?= sml2
SCALE       ?= 0.1
REPS        ?= 3
WARMUP      ?= 1
PARTITIONS  ?= 8
BENCH_DIR   := $(WORK_DIR)/bench
BENCH_ARGS   = --algo $(ALGO) --scale $(SCALE) --reps $(REPS) --warmup $(WARMUP) --partitions $(PARTITIONS) --data $(BENCH_DIR)/data
VANILLA_PORT ?= 7078

# Appends the RESULT rows of log $(1) to the CSV (header on first use), then prints the summary.
bench-collect = rc=$$?; mkdir -p $(BENCH_DIR); \
	[ -s $(BENCH_DIR)/results.csv ] || grep -m1 '^HEADER,' $(1) | cut -d, -f2- > $(BENCH_DIR)/results.csv; \
	grep '^RESULT,' $(1) | cut -d, -f2- >> $(BENCH_DIR)/results.csv; \
	echo "==== bench summary (full log: $(1)) ===="; \
	grep '^RESULT,' $(1) | cut -d, -f4-5,9-13 \
	  | awk -F, '{ printf "%-8s %-7s rep %-2s warmup=%-5s %9ss  %s=%s\n", $$1, $$2, $$3, $$4, $$5, $$6, $$7 }'; \
	$(call summary,$(1)); \
	echo "results: $(BENCH_DIR)/results.csv"; \
	exit $$rc

# SML-1 / SML-2 on the cluster of step 3, driver in an enclave.
bench: all $(KEYS_FILE)
	@case "$(MODE)" in sml1|sml2) ;; *) echo "MODE must be sml1 or sml2 (vanilla: make bench-vanilla)"; exit 1;; esac
	mkdir -p $(BENCH_DIR)
	set -o pipefail; SPARK_LOCAL_IP=$(MASTER_HOST) SOTERIA_MASTER_KEYS_FILE=$(KEYS_FILE) SOTERIA_RUNNER=$(GRAMINE) \
	  $(GRAMINE) java $(JVM_OPTS) -cp '$(SPARK_CP)' $(CLUSTER_CONF) \
	  soteria.bench.Bench --mode $(MODE) $(BENCH_ARGS) 2>&1 | tee $(BENCH_DIR)/$(MODE).log; \
	$(call bench-collect,$(BENCH_DIR)/$(MODE).log)

# Vanilla Spark baseline: its own native Master and Worker (port
# $(VANILLA_PORT), so the enclave cluster can keep running), the same number
# of 1-core executors, a native driver, plain Parquet and MLlib.
bench-vanilla: dist check-env
	mkdir -p $(BENCH_DIR)
	set -o pipefail; \
	SPARK_LOCAL_IP=$(MASTER_HOST) $(JAVA_REAL)/bin/java $(JVM_OPTS) -cp '$(SPARK_CP)' org.apache.spark.deploy.master.Master \
	  --host $(MASTER_HOST) --port $(VANILLA_PORT) --webui-port 0 > $(BENCH_DIR)/vanilla-master.log 2>&1 & m=$$!; \
	sleep 5; \
	env -u SOTERIA_MASTER_KEYS -u SOTERIA_MASTER_KEYS_FILE JAVA_HOME=$(JAVA_REAL) SPARK_HOME=$(SPARK_REAL) SPARK_SCALA_VERSION=2.12 SPARK_LOCAL_IP=$(MASTER_HOST) \
	  $(JAVA_REAL)/bin/java $(JVM_OPTS) -cp '$(SPARK_CP)' org.apache.spark.deploy.worker.Worker \
	  --host $(MASTER_HOST) --cores $(WORKER_CORES) --memory $(WORKER_MEM) --webui-port 0 \
	  --work-dir $(BENCH_DIR)/vanilla-worker spark://$(MASTER_HOST):$(VANILLA_PORT) > $(BENCH_DIR)/vanilla-worker.log 2>&1 & w=$$!; \
	sleep 8; \
	SPARK_LOCAL_IP=$(MASTER_HOST) SOTERIA_RUNNER=native $(JAVA_REAL)/bin/java $(JVM_OPTS) -cp '$(SPARK_CP)' \
	  -Dspark.master=spark://$(MASTER_HOST):$(VANILLA_PORT) -Dspark.driver.host=$(MASTER_HOST) -Dspark.ui.enabled=false \
	  -Dspark.executor.cores=1 -Dspark.executor.memory=1g -Dspark.cores.max=$(WORKER_CORES) \
	  -Dspark.executor.extraJavaOptions='$(JVM_BASE_OPTS)' -Dspark.executor.extraClassPath='$(SOTERIA_DIR)/*' \
	  soteria.bench.Bench --mode vanilla $(BENCH_ARGS) 2>&1 | tee $(BENCH_DIR)/vanilla.log; \
	rc=$$?; kill $$w $$m 2>/dev/null; (exit $$rc); \
	$(call bench-collect,$(BENCH_DIR)/vanilla.log)

# Plots from results.csv (needs python3-matplotlib).
bench-plot:
	python3 $(ROOT)/bench/plot.py $(BENCH_DIR)/results.csv --out $(BENCH_DIR)/plots

clean:
	$(RM) java.manifest java.manifest.sgx java.sig hello/*.class
	$(RM) -r $(SOTERIA_DIR) $(WORK_DIR)
