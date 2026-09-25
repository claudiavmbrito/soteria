# Running SOTERIA's Spark JVMs under Gramine

`java.manifest.template` is one Gramine manifest for every SOTERIA JVM that
handles data: the driver and the executors of the enclave Worker.

| Process | Runs |
|---|---|
| Spark Master | natively (schedules only, sees no data) |
| Enclave Worker daemon | natively; advertises the `enclave` resource and launches its executors through `work/enclave-jdk/bin/java`, a wrapper that starts each executor as a fresh Gramine process |
| Enclave executors | in Gramine (an SGX enclave with `SGX=1`) |
| Untrusted Worker and its executors | natively, with no keys |
| Driver (`run-check`) | in Gramine |

Starting every executor from the host, instead of forking it inside a
Gramine-hosted Worker, avoids copying a multi-GB JVM on each launch (Gramine
implements fork by checkpointing the whole process).

Every target runs under `gramine-direct` by default. That needs no SGX, and it
checks most of the setup: mounts, file lists, and whether the JVM starts under
Gramine. With `SGX=1` the same targets run under `gramine-sgx`, with a signed
enclave.

## Prerequisites

```bash
scripts/install_build_deps.sh   # OpenJDK 17, sbt, make
scripts/install_spark.sh        # Spark into /opt/spark
scripts/install_gramine.sh      # Gramine and an enclave signing key
```

Makefile variables (defaults in brackets):

| Variable | Meaning |
|---|---|
| `JAVA_HOME` [`/usr/lib/jvm/java-17-openjdk`] | JDK 17; on Ubuntu use `/usr/lib/jvm/java-17-openjdk-amd64` |
| `SPARK_HOME` [`/opt/spark`] | Spark binary distribution |
| `SGX` [0] | 1 = sign and run with `gramine-sgx` |
| `DEBUG` [0] | 1 = debug enclave and Gramine debug log |
| `ENCLAVE_SIZE` [8G], `JVM_HEAP` [2g] | enclave size; keep the heap well below it |
| `EDMM` [0] | 1 on SGX2 CPUs: the enclave grows memory on demand (also enables `sgx.use_exinfo`) |
| `WORKER_CORES` [2], `WORKER_MEM` [4g] | resources per Worker |

## Bring-up order

Run each step, and send the full output if a step fails.

```bash
cd gramine

# 1. A trivial JVM program under Gramine.
make hello

# 2. Load Spark's classes.
make spark-version

# 3. A cluster: native Master, enclave Worker, untrusted Worker (three terminals).
make start-master
make start-worker-enclave
make start-worker-native

# 4. The SML-2 placement check, with the driver in an enclave.
make run-check

# 5. Benchmarks: vanilla Spark vs SML-1 vs SML-2 (see ../bench/README.md).
make bench MODE=sml2
make bench MODE=sml1
make bench-vanilla
make bench-plot
```

Step 4 expects the same `PASS` line as `scripts/local-cluster/run.sh`. It
saves its full output to `work/run-check.log` and ends with a short summary;
when asking for help, send that summary and the first errors of the executors:
`grep -h -m5 -E "ERROR|Exception" work/*-worker/app-*/*/stderr`. The
first of `start-worker-enclave` / `run-check` creates `work/master.keys`; the
driver and the enclave executors read the master key from it, and the
untrusted Worker never gets it. No key needs to be passed between terminals.

Once SGX is enabled in the BIOS and `scripts/check_sgx.sh` passes, repeat the
steps with `SGX=1` (and `EDMM=1` on SGX2). With `SGX=1` the enclave
Worker advertises the `enclave` resource when `/dev/sgx_enclave` exists
(`SOTERIA_ENCLAVE_RUNNER=gramine-sgx`); with `gramine-direct` the Makefile sets
`SOTERIA_FAKE_ENCLAVE=1` instead.

## Known open points

- **INSECURE bring-up settings.** `loader.insecure__use_cmdline_argv` lets the
  host choose the JVM arguments, and the master key comes from a host file
  (`work/master.keys`, or `SOTERIA_MASTER_KEYS` if set). Both are replaced by fixed arguments and RA-TLS secret provisioning
  (DCAP) with the key server, after the bring-up works.
- **Dynamic loader.** On Rocky, `/lib64` (host libraries) is mounted next to
  Gramine's patched glibc in `/lib`. If step 1 fails while loading libraries,
  run with `DEBUG=1` and send the log.
- **Symlinked JDK config (RHEL/Rocky).** The JDK's `conf/` directory and the
  crypto-policies `java.config` are symlinks into `/etc` and `/usr/share`. The
  Makefile resolves them, and the manifest mounts the real locations at the
  paths the JVM opens (`Error loading java.security file` otherwise).
- **Quieter logs.** `log4j2.properties` (passed to the driver and executors)
  silences the expected warnings listed below, so real errors stand out.
- **Harmless messages in Gramine JVMs.** Hadoop probes `setsid` by forking at
  startup; inside Gramine that fork can fail (`process creation failed`) and is
  ignored. At shutdown Spark tries `rm` in a child process the same way and
  falls back to deleting from Java ("Falling back to Java IO way"). Hadoop's
  `chmod`/`chown` child processes, which would be fatal when writing output,
  are replaced by `soteria.io.NioLocalFileSystem` (set by every SOTERIA session). Netty
  still warns that it cannot list network interfaces (`SIOCGIFCONF`); Spark
  itself uses `SPARK_LOCAL_IP`.
- **Private `/tmp` in enclaves.** Each enclave JVM has its own in-enclave
  `/tmp` (the manifest's tmpfs mount), which holds Spark's shuffle files. Every
  SOTERIA session therefore sets `spark.shuffle.readHostLocalDisk=false`, so
  shuffle blocks are always fetched from the executor that wrote them
  (`NoSuchFileException … shuffle_*.index` otherwise).
- **Memory.** The JVM reserves heap, metaspace, code cache and thread stacks
  up front; if the enclave runs out of memory, raise `ENCLAVE_SIZE` or lower
  `JVM_HEAP`.
