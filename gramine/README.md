# Running SOTERIA's Spark JVMs under Gramine

`java.manifest.template` is one Gramine manifest for every SOTERIA JVM that
must run in an enclave: the driver, the enclave Worker and the executors that
Worker spawns (as child enclaves of the same manifest). The Spark Master and
the untrusted Worker run natively.

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
SOTERIA_MASTER_KEYS="soteria-master:$(head -c 16 /dev/urandom | base64)" make run-check
```

Step 4 expects the same `PASS` line as `scripts/local-cluster/run.sh`. The
enclave Worker needs the same `SOTERIA_MASTER_KEYS` in its environment as the
driver, so start it with the variable set as well.

Once SGX is enabled in the BIOS and `scripts/check_sgx.sh` passes, repeat the
steps with `SGX=1` (and `EDMM=1` on SGX2). Under `gramine-sgx` the enclave
Worker advertises the `enclave` resource because `/dev/attestation` exists;
under `gramine-direct` the Makefile sets `SOTERIA_FAKE_ENCLAVE=1` instead.

## Known open points

- **INSECURE bring-up settings.** `loader.insecure__use_cmdline_argv` lets the
  host choose the JVM arguments, and `SOTERIA_MASTER_KEYS` is passed in from
  the host. Both are replaced by fixed arguments and RA-TLS secret provisioning
  (DCAP) with the key server, after the bring-up works.
- **Dynamic loader.** On Rocky, `/lib64` (host libraries) is mounted next to
  Gramine's patched glibc in `/lib`. If step 1 fails while loading libraries,
  run with `DEBUG=1` and send the log.
- **Symlinked JDK config (RHEL/Rocky).** The JDK's `conf/` directory and the
  crypto-policies `java.config` are symlinks into `/etc` and `/usr/share`. The
  Makefile resolves them, and the manifest mounts the real locations at the
  paths the JVM opens (`Error loading java.security file` otherwise).
- **Memory.** The JVM reserves heap, metaspace, code cache and thread stacks
  up front; if the enclave runs out of memory, raise `ENCLAVE_SIZE` or lower
  `JVM_HEAP`.
