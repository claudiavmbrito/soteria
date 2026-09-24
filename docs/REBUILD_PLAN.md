# SOTERIA: rebuild on a modern SGX + Gramine stack

## Context

SOTERIA (Brito et al., IEEE Access 2023) is privacy-preserving ML on Apache Spark using Intel SGX via Graphene/Gramine. It has two designs:
- **SML-1 / SOTERIA-B (baseline):** Master and Workers run entirely inside enclaves.
- **SML-2 / SOTERIA-P (partitioned):** each node runs two workers, one inside an enclave and one outside. Only the operations that touch raw data run in the enclave; the untrusted side sees only statistical or aggregated data. The proof's leakage function `L` for SML-2 also "reveals statistical data".

The formal protocol is in `proofs/Soteria_Proof.pdf`:
- Data is pre-encrypted in storage G with authenticated encryption (AES-GCM). This gives integrity against dataset manipulation (Appendix B.2).
- The Client sends `(manifest m, script s, key k)` to the Master over an attested secure channel, and the Master forwards it to the Workers the same way.
- Workers `uGet` the ciphertext, decrypt inside the enclave, run `s.Run`, and exchange intermediate results over secure channels.

### What's in the repo today (why it doesn't work)

- **`src/main/scala/**` is a mock, not an implementation.**
  - `SoteriaCore.executeInEnclave` only `println`s "[SOTERIA-SGX]" and runs the same Spark code. Nothing is split between an enclave and the untrusted side.
  - `loadEncryptedDataset` reads plaintext Parquet and marks it `isEncrypted = true`. `EncryptionUtils` is never used.
  - `masterKey` is a fresh random key per session. There's no provisioning or attestation.
  - `SoteriaML.scala:93` calls `session.loadEncryptedDataset("dummy")` with no type or encoder, so it **won't compile**.
  - LR, ALS and K-Means reimplement MLlib naively with driver-side loops. Extended ML just calls MLlib inside the fake wrapper.
  - `SecurityUtils` shuffle and aggregate are no-ops security-wise. There are no tests.
- **The real prototype was the Graphene manifest plus the Makefile** (`graphene-sgx-spark/spark/`), and it's all obsolete:
  - Graphene v1.0 key=value manifest syntax, `pal_loader`, `pal-sgx-get-token`
  - Out-of-tree SGX driver 2.6 and SDK 2.8, EPID (end of life; now DCAP)
  - Ubuntu 18.04, Oracle Java 8, Cloudera CDH 6.3.2 hard-coded paths
  - Spark 2.3.4 in `scripts/build.sh`, but Spark 3.2.0 in `build.sbt`
- `build.sbt` pulls `slf4j-log4j12`, which conflicts with Spark 3.3+ log4j2.

**Goal (agreed):** keep the paper's design, port it to current SGX + Gramine, and make it work for real on SGX2/FLC hardware. First deliver a working, tested library. Then reproduce the paper's evaluation for the 8 algorithms: ALS, Naive Bayes, GBT, K-Means, LDA, Linear Regression, Logistic Regression and PCA, each against vanilla Spark.

## Target stack

| Area | Now | Target |
|---|---|---|
| OS and kernel | Ubuntu 18.04 | Ubuntu 22.04/24.04; in-kernel SGX driver (≥5.11, `/dev/sgx_enclave`) |
| SGX | SDK 2.8, EPID | Current SGX SDK/PSW + **DCAP** (PCCS, `libsgx-dcap-quote-verify`) |
| Library OS | Graphene v1.0 | **Gramine ≥1.8** (TOML/Jinja manifests, `gramine-manifest`, `gramine-sgx-sign`, EDMM on SGX2) |
| JVM | Oracle Java 8 | OpenJDK 17 |
| Spark and Scala | 2.3.4 / 3.2.0; Scala 2.12 | **Spark 3.5.x, Scala 2.12** (Scala 2.13 optional later) |
| Cluster | Cloudera CDH 6.3.2 | Spark standalone (Docker Compose for dev); YARN/K8s optional later |
| Benchmarks | HiBench (old jar) | Current HiBench, or an equivalent in-repo data generator |

## Design

### 1. Encrypted storage (proof: Setup, Figure 1)
- Use **Parquet modular encryption** (built into Spark 3.2+, AES-GCM). It replaces the custom AES-GCM described in the README, and GCM gives the integrity property from Appendix B.2.
- Add a `soteria-encrypt` CLI/job that encrypts datasets offline: the client-side Setup.
- Implement a custom `KmsClient` (`soteria.crypto.SoteriaKmsClient`). It unwraps data keys only when running inside an attested enclave. Keys come from the secret-provisioning step below and are never present on untrusted executors.
- Keep `EncryptionUtils` (AES-GCM) for small blobs, such as model export. Fix it to use one `SecureRandom` and take associated data (AAD).

### 2. Attestation and key provisioning (proof: `init`, secure channels)
- **Gramine RA-TLS secret provisioning** (`ra_tls_secret_prov`, DCAP):
  - A small client-side **Key Server** (the paper's "Client" C) verifies MRENCLAVE/MRSIGNER plus the TCB status.
  - It then releases the dataset master key. Gramine injects the key into the enclave through the manifest (`sgx.remote_attestation = "dcap"`, secret-prov env vars).
- Spark internal channels (driver↔executor, shuffle):
  - Enable `spark.authenticate`, `spark.network.crypto.enabled` and `spark.io.encryption.enabled` on trusted components. This protects shuffle and spill data leaving the enclave.
  - Keep the shared secret in the provisioned-secret path, not on disk.

### 3. Enclave packaging (replaces `graphene-sgx-spark/`)
- New directory `gramine/` containing:
  - `spark-java.manifest.template`: Jinja/TOML, based on Gramine's OpenJDK example. Details:
    - `loader.entrypoint`, `libos.entrypoint = /usr/lib/jvm/java-17…/bin/java`
    - `sgx.enclave_size` (e.g. 16G–32G), `sgx.max_threads`, `sgx.edmm_enable = true`
    - `sgx.trusted_files` for the JDK, the Spark jars and `soteria-assembly.jar`
    - `sgx.allowed_files` only for `/tmp` spill paths, which are protected by Spark I/O encryption
    - `fs.mounts` of type `encrypted` for local scratch
  - `Makefile`: `make SGX=1` builds and signs the manifest; `make DEBUG=1`; `gramine-direct` for non-SGX smoke runs.
  - Launch targets `start-master-sgx`, `start-worker-sgx` and `start-worker-native`. These replace the CDH-path targets.
- Reuse the JVM flags from the old Makefile (Xms/Xmx, CodeCache, Metaspace, `-XX:+PreserveFramePointer`). Drop `-XX:+UseMembar`, which Java 17 no longer supports.

### 4. SML-2 computation partitioning with real Spark scheduling
The paper's "double worker" becomes two Spark Workers per node:
- **Trusted worker:** runs under `gramine-sgx`. It advertises a custom resource `soteria.enclave=1` (`spark.worker.resource.soteria.enclave.amount` plus a discovery script that checks for `/dev/attestation`).
- **Untrusted worker:** native, with no such resource.

Routing uses Spark **stage-level scheduling** (`ResourceProfileBuilder` + `rdd.withResources`). In standalone mode this needs `spark.dynamicAllocation.enabled=true` and `shuffleTracking.enabled=true`.
- `EnclaveProfile`: requires task resource `soteria.enclave`. Stages that read or decrypt raw data run here.
- `UntrustedProfile`: default executors. Stages that only consume aggregates run here.

The **driver** (the paper's Master, holding the model) always runs inside Gramine.

This replaces `SoteriaCore.executeWithPartitioning(operation: String, …)` with a typed API:
```scala
trait SoteriaStage
def sensitive[T](rdd: RDD[T]): RDD[T]       // withResources(EnclaveProfile)
def nonSensitive[T](rdd: RDD[T]): RDD[T]    // withResources(UntrustedProfile)
```
Also add a `LeakageAuditor` that checks at runtime that no RDD derived from decrypted data reaches an untrusted stage without first going through a declared "statistic" operation. This enforces the leakage function `L`.

**Per-algorithm split.** Enclave: decrypt plus the per-record map. Outside: combines of aggregated stats and the driver-side optimizer.
| Algorithm | Enclave (sensitive) | Untrusted (statistics only) |
|---|---|---|
| LR / Linear | decrypt, per-partition gradient + loss sums | treeAggregate combine levels, convergence checks |
| K-Means | decrypt, point→centroid assignment, per-partition (sum, count) | reduceByKey over centroid stats |
| Naive Bayes | decrypt, per-partition label/feature counts | aggregation of counts |
| PCA | decrypt, per-partition Gram matrix / covariance terms | summing Gram matrices (the SVD runs in the driver, which is in the enclave) |
| ALS | decrypt ratings, in-block normal equations | factor-block shuffle of factors (not ratings) |
| GBT | decrypt, per-partition split statistics / histograms | histogram aggregation |
| LDA | decrypt, per-document sufficient stats | topic-term count aggregation |
In **SML-1**, all profiles map to enclave executors. The mode is a single config switch: `soteria.mode = SML1 | SML2`.

**How:**
- Wrap MLlib where possible. Many MLlib trainers internally call `treeAggregate` on the input RDD, so feeding them an RDD tagged `sensitive` runs the whole trainer in the enclave: effectively SML-1 per algorithm.
- For true SML-2, write thin partitioned trainers for LR, Linear, K-Means, NB and PCA. They reuse MLlib's `Aggregator` / `BLAS` / `MultivariateOnlineSummarizer`, but split the stages explicitly.
- ALS, GBT and LDA start as enclave-only (SML-1 style) behind the same API. Explicit partitioning for them comes in a later phase.
- Delete the hand-rolled SGD/ALS/Gaussian-elimination code in `SoteriaML.scala`.

## Phases

**Phase 0: Make it build and tell the truth (no SGX needed)**: done
- `build.sbt`:
  - Spark 3.5.x, Java 17, `% "provided"` on Spark dependencies
  - Remove `slf4j-log4j12`
  - Add `scalafmt` and ScalaTest
- Fix the compile error in `SoteriaML.scala:93`.
- Remove the fake "[SOTERIA-SGX]" printlns and the `isEncrypted` flag.
- Add a `SoteriaSuite` with local-mode Spark tests.
- Add a GitHub Actions workflow running `sbt test`.

**Phase 1: Real encrypted I/O**
- Add `soteria.crypto` (KmsClient, encrypt CLI).
- Tests:
  - An encrypt→read round-trip.
  - A tampered ciphertext makes the read fail. This is the B.2 integrity property.
  - An executor without the key can't read the data.

**Phase 2: Partitioning API + algorithms (local and gramine-direct)**
- Add `SoteriaSession` (mode, profiles), `sensitive`/`nonSensitive` and `LeakageAuditor`.
- Port all 8 algorithms. Each needs an accuracy test against vanilla MLlib on synthetic data.
- Local tests fake the enclave resource with a discovery script, so the scheduling split can be asserted from `SparkListener` task→executor events.

**Phase 3: Gramine + SGX + DCAP on the hardware**
- New `scripts/install_sgx.sh`: in-kernel driver check, PSW/DCAP, PCCS.
- New `scripts/install_gramine.sh` and `scripts/install_spark.sh`. Retire `install_cluster.sh` (CDH) and the old `graphene-sgx-spark/`, keeping them under `legacy/`.
- Add `gramine/` manifests plus the Makefile, and `keyserver/` (RA-TLS secret-prov server config).
- Bring-up order:
  1. `HelloWorld` (reuse `graphene-sgx-spark/java_tests/HelloWorld.java`)
  2. `spark-shell` in the enclave
  3. Master + trusted Worker
  4. Mixed trusted/untrusted Workers
  5. The full SML-2 job

**Phase 4: Paper reproduction**
- Add `bench/`: data generators or HiBench configs for the 8 algorithms at the paper's input sizes. The old Makefile's ALS options are one data point (30k users / 40k products, rank 10).
- Run each algorithm in three setups: vanilla, SML-1 and SML-2.
- Collect runtime, enclave stats (`sgx.enable_stats`), EPC paging and accuracy into CSV, and add a plotting script.
- Update the README (install, run, results) and mark v2.0.

## Critical files
- Rewrite: `build.sbt`, `project/*`, `src/main/scala/soteria/core/SoteriaCore.scala`, `src/main/scala/soteria/ml/*.scala`, `src/main/scala/examples/SoteriaExamples.scala`
- New:
  - `src/main/scala/soteria/crypto/*`, `src/main/scala/soteria/partition/*`, `src/test/scala/**`
  - `gramine/`, `keyserver/`, `bench/`
  - `.github/workflows/ci.yml`
- Replace: `scripts/install_sgx.sh`, `scripts/build.sh`, `scripts/install_cluster.sh`
- Move to `legacy/`: `graphene-sgx-spark/`

## Verification
- **Phases 0–2 (CI, no SGX):** `sbt compile test`.
  - Accuracy parity with MLlib, within tolerance, for all 8 algorithms.
  - The tampered-data test fails closed.
  - The scheduling test proves sensitive stages ran only on executors with the enclave resource.
  - `LeakageAuditor` rejects a deliberately leaky pipeline.
- **Phase 3 (SGX2 machine):**
  - `is-sgx-available` passes. `gramine-sgx` HelloWorld runs.
  - Key Server logs a verified DCAP quote before releasing the key. A modified jar changes MRENCLAVE, so the key is refused.
  - `soteria-examples` runs in SML-1 and SML-2 on a two-node (or two-worker, single-node) standalone cluster.
- **Phase 4:** the bench scripts produce CSVs and plots for vanilla / SML-1 / SML-2. Compare the trends with the paper's figures.

## Workflow
Develop on `claude/charming-knuth-392bk8`, with one commit per phase or sub-step, and push after each phase passes its checks.
