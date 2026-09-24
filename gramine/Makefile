# Runs the SOTERIA Spark JVM under Gramine.
#   make <target>          gramine-direct (no SGX needed; checks the setup)
#   make SGX=1 <target>    gramine-sgx (signed enclave)
# See README.md for the bring-up order.

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
JVM_BASE_OPTS := -XX:+UseSerialGC -XX:-UseContainerSupport -XX:-UsePerfData \
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

.PHONY: all check-env dist hello spark-version start-master start-worker-enclave start-worker-native run-check clean

all: java.manifest $(SGX_FILES)

check-env:
	@test -x "$(JAVA_REAL)/bin/java" || { echo "JAVA_HOME=$(JAVA_HOME) has no bin/java"; exit 1; }
	@test -d "$(SPARK_REAL)/jars" || { echo "SPARK_HOME=$(SPARK_HOME) not found (scripts/install_spark.sh)"; exit 1; }
	@command -v $(GRAMINE) >/dev/null || { echo "$(GRAMINE) not found (scripts/install_gramine.sh)"; exit 1; }

# SOTERIA jars built by sbt (sbt package Test/package), copied so that only
# they, not the whole target/ tree, are measured into the enclave.
dist:
	@ls $(ROOT)/target/scala-2.12/soteria_2.12-*.jar >/dev/null 2>&1 || (cd $(ROOT) && sbt -batch package Test/package)
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

$(ENCLAVE_JDK)/bin/java: Makefile
	mkdir -p $(@D)
	printf '#!/bin/sh\n# Generated by gramine/Makefile: runs a Spark executor under %s.\nexec %s %s/java "$$@"\n' \
	  $(GRAMINE) $(GRAMINE) $(CURDIR) > $@
	chmod +x $@

start-worker-enclave: all $(ENCLAVE_JDK)/bin/java
	printf 'spark.worker.resource.enclave.amount %s\nspark.worker.resource.enclave.discoveryScript %s\n' \
	  $(WORKER_CORES) $(DISCOVERY) > $(WORK_DIR)/enclave-worker.conf
	JAVA_HOME=$(ENCLAVE_JDK) SPARK_HOME=$(SPARK_REAL) SPARK_SCALA_VERSION=2.12 SPARK_LOCAL_IP=$(MASTER_HOST) SOTERIA_ENCLAVE_SLOTS=$(WORKER_CORES) \
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

# Step 4: the SML-2 placement check, with the driver in an enclave.
run-check: all
	SPARK_LOCAL_IP=$(MASTER_HOST) $(GRAMINE) java $(JVM_OPTS) -cp '$(SPARK_CP)' \
	  -Dspark.master=$(MASTER) -Dspark.driver.host=$(MASTER_HOST) -Dspark.ui.enabled=false \
	  -Dspark.executor.cores=1 -Dspark.executor.memory=1g \
	  -Dspark.executor.resource.enclave.amount=1 -Dspark.task.resource.enclave.amount=1 \
	  -Dspark.dynamicAllocation.enabled=true -Dspark.dynamicAllocation.shuffleTracking.enabled=true \
	  -Dspark.dynamicAllocation.minExecutors=2 -Dspark.dynamicAllocation.maxExecutors=2 \
	  -Dspark.soteria.untrusted.executor.memory=1g \
	  -Dspark.executor.extraJavaOptions='$(JVM_BASE_OPTS)' \
	  -Dspark.executor.extraClassPath='$(SOTERIA_DIR)/*' \
	  soteria.it.SchedulingCheck $(WORK_DIR)/data

clean:
	$(RM) java.manifest java.manifest.sgx java.sig hello/*.class
	$(RM) -r $(SOTERIA_DIR) $(WORK_DIR)
