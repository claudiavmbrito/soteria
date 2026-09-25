JAVA_HOME=/usr/lib/jvm/java-8-oracle-cloudera/bin/java
GRAPHENEDIR ?= ../../../..
INSTALLDIR = $(JAVA_HOME)

JVM_INITIAL_CODE_CACHE_SIZE=32m
JVM_RESERVED_CODE_CACHE_SIZE=32m
JVM_XMS=512m
JVM_XMX=512m
JVM_COMPRESSED_CLASS_SPACE_SIZE=32m
JVM_MAX_METASPACE_SIZE=128m

SPARK_MASTER_WEBUI_PORT = 8080
SPARK_MASTER_HOST = 127.0.0.1
SPARK_MASTER_PORT=7077


OPTION = "--numUsers 30000 \
	--numProducts 40000 \
	--rank 10 \
	--numRecommends 20 \
	--numIterations 20 \
	--kryo false \
	--implicitPrefs true \
	--numProductBlocks -1 \
	--numUserBlocks -1 \
	--lambda 1.0"


GRAPHENEKEY ?= $(GRAPHENEDIR)/Pal/src/host/Linux-SGX/signer/enclave-key.pem

ifeq ($(DEBUG),1)
GRAPHENEDEBUG = inline
else
GRAPHENEDEBUG = none
endif


.PHONY: all
all: spark.manifest pal_loader
ifeq ($(SGX),1)
all: spark.manifest.sgx spark.token spark.sig
endif


GLIBC_DEPS = linux-vdso /lib64/ld-linux-x86-64 libc libm librt libdl libpthread

spark.sig: java.sig

spark.manifest: spark.manifest.template
	sed -e 's|$$(GRAPHNEDIR)|'"$(GRAPHENEDIR)"'|g' \
		-e 's|$$(GRAPHENEDEBUG)|'"$(GRAPHENEDEBUG)"'|g' \
		-e 's|$$(HOME)|'"$(HOME)"'|g' \
		$< > $@

# Generating the SGX-specific manifest (.manifest.sgx), the enclave signature,
# and the token for enclave initialization.
spark.manifest.sgx: spark.manifest
	$(GRAPHENEDIR)/Pal/src/host/Linux-SGX/signer/pal-sgx-sign \
		-libpal $(GRAPHENEDIR)/Runtime/libpal-Linux-SGX.so \
		-key $(GRAPHENEDIR)/Pal/src/host/Linux-SGX/signer/enclave-key.pem \
		-manifest $< -output $@

spark.sig: spark.manifest.sgx

spark.token: spark.sig
	$(GRAPHENEDIR)/Pal/src/host/Linux-SGX/signer/pal-sgx-get-token \
		-output spark.token -sig spark.sig
		

java.manifest: spark.manifest
	cp $< $@
	sed -i '/trusted_children/d' $@

java.sig: java.manifest
	$(GRAPHENEDIR)/Pal/src/host/Linux-SGX/signer/pal-sgx-sign \
		-libpal $(GRAPHENEDIR)/Runtime/libpal-Linux-SGX.so \
		-key $(GRAPHENEDIR)/Pal/src/host/Linux-SGX/signer/enclave-key.pem \
		-output java.manifest.sgx -exec /usr/lib/jvm/java-8-oracle-cloudera/bin/java -manifest $<

# Extra executables
pal_loader:
	ln -s $(GRAPHENEDIR)/Runtime/pal_loader $@

.PHONY: start-spark-master
start-spark-master-native: 
	java \
	-cp  '/opt/cloudera/parcels/CDH-6.3.2-1.cdh6.3.2.p0.1605554/lib/spark/conf/:/opt/cloudera/parcels/CDH-6.3.2-1.cdh6.3.2.p0.1605554/lib/spark/jars/*' \
	org.apache.spark.deploy.master.Master \
	--host $(SPARK_MASTER_HOST) \
	--port ${SPARK_MASTER_PORT} \
	--webui-port ${SPARK_MASTER_WEBUI_PORT}

.PHONY: start-spark-master-graphene
start-spark-master-graphene:
	./pal_loader spark.manifest \
	-Xms${JVM_XMS} \
	-Xmx${JVM_XMX} \
	-XX:InitialCodeCacheSize=${JVM_INITIAL_CODE_CACHE_SIZE} \
	-XX:ReservedCodeCacheSize=${JVM_RESERVED_CODE_CACHE_SIZE} \
	-XX:CompressedClassSpaceSize=${JVM_COMPRESSED_CLASS_SPACE_SIZE} \
	-XX:MaxMetaspaceSize=${JVM_MAX_METASPACE_SIZE} \
	-XX:+UseCompressedClassPointers \
	-XX:+PreserveFramePointer \
	-XX:+UseMembar \
	-Djava.library.path=/opt/cloudera/parcels/CDH-6.3.2-1.cdh6.3.2.p0.1605554/lib/spark/lib/ \
	-cp  '/opt/cloudera/parcels/CDH-6.3.2-1.cdh6.3.2.p0.1605554/lib/spark/conf/:/opt/cloudera/parcels/CDH-6.3.2-1.cdh6.3.2.p0.1605554/lib/spark/jars/*' \
	org.apache.spark.deploy.master.Master \
	--host $(SPARK_MASTER_HOST) \
	--port ${SPARK_MASTER_PORT} \
	--webui-port ${SPARK_MASTER_WEBUI_PORT}

.PHONY: start-spark-slave-graphene
start-spark-slave-graphene:
	./pal_loader spark.manifest \
	-Xms${JVM_XMS} \
	-Xmx${JVM_XMX} \
	-XX:InitialCodeCacheSize=${JVM_INITIAL_CODE_CACHE_SIZE} \
	-XX:ReservedCodeCacheSize=${JVM_RESERVED_CODE_CACHE_SIZE} \
	-XX:CompressedClassSpaceSize=${JVM_COMPRESSED_CLASS_SPACE_SIZE} \
	-XX:MaxMetaspaceSize=${JVM_MAX_METASPACE_SIZE} \
	-XX:+UseCompressedClassPointers \
	-XX:+PreserveFramePointer \
	-XX:+UseMembar \
	-XX:MaxHeapSize=512m \
	-Djava.library.path=/opt/cloudera/parcels/CDH-6.3.2-1.cdh6.3.2.p0.1605554/lib/spark/lib/ \
	-cp  '/opt/cloudera/parcels/CDH-6.3.2-1.cdh6.3.2.p0.1605554/lib/spark/conf/:/opt/cloudera/parcels/CDH-6.3.2-1.cdh6.3.2.p0.1605554/lib/spark/jars/*' \
	org.apache.spark.deploy.worker.Worker \
    spark://${SPARK_MASTER_HOST}:${SPARK_MASTER_PORT}

.PHONY: start-spark-slaves
start-spark-slaves-native:
	java \
	-cp  /opt/cloudera/parcels/CDH-6.3.2-1.cdh6.3.2.p0.1605554/lib/spark/jars/\* \
	org.apache.spark.deploy.worker.Worker \
    spark://${SPARK_MASTER_HOST}:${SPARK_MASTER_PORT}

.PHONY: java-HelloWorld
java-HelloWorld:
	./pal_loader spark.manifest \
	-Xms${JVM_XMS} \
	-Xmx${JVM_XMX} \
	-XX:InitialCodeCacheSize=${JVM_INITIAL_CODE_CACHE_SIZE} \
	-XX:ReservedCodeCacheSize=${JVM_RESERVED_CODE_CACHE_SIZE} \
	-XX:CompressedClassSpaceSize=${JVM_COMPRESSED_CLASS_SPACE_SIZE} \
	-XX:MaxMetaspaceSize=${JVM_MAX_METASPACE_SIZE} \
	-XX:+UseCompressedClassPointers \
	-XX:+PreserveFramePointer \
	-XX:+UseMembar \
	-XX:MaxHeapSize=512m \
	HelloWorld

.PHONY: spark-ALS
java-ALS:
	./pal_loader spark.manifest \
	-Xms${JVM_XMS} \
	-Xmx${JVM_XMX} \
	-XX:InitialCodeCacheSize=${JVM_INITIAL_CODE_CACHE_SIZE} \
	-XX:ReservedCodeCacheSize=${JVM_RESERVED_CODE_CACHE_SIZE} \
	-XX:CompressedClassSpaceSize=${JVM_COMPRESSED_CLASS_SPACE_SIZE} \
	-XX:MaxMetaspaceSize=${JVM_MAX_METASPACE_SIZE} \
	-XX:+UseCompressedClassPointers \
	-XX:+PreserveFramePointer \
	-XX:+UseMembar \
	-XX:MaxHeapSize=512m \
	-cp '/opt/cloudera/parcels/CDH-6.3.2-1.cdh6.3.2.p0.1605554/lib/spark/conf/:/opt/cloudera/parcels/CDH-6.3.2-1.cdh6.3.2.p0.1605554/lib/spark/jars/*' \
	org.apache.spark.deploy.SparkSubmit \
	--master yarn-client \
	--properties-file /home/HiBench/report/als/spark/conf/sparkbench/spark.conf \
	--class com.intel.hibench.sparkbench.ml.ALSExample \
	--num-executors 7 \
	--executor-cores 4 \
	--executor-memory 4g \
	/home/HiBench/sparkbench/assembly/target/sparkbench-assembly-7.1-SNAPSHOT-dist.jar $(OPTION) hdfs://192.168.112.111:8020/HiBench/ALS/Input



.PHONY: clean
clean:
	$(RM) *.manifest *.manifest.sgx *.token *.sig pal_loader 

