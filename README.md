# SOTERIA: Privacy-Preserving Machine Learning for Apache Spark

**SOTERIA** is a privacy-preserving machine learning solution developed on top of [Apache Spark](https://github.com/apache/spark) using Intel SGX for secure computation. It employs computation partitioning to perform sensitive computing tasks within secure enclaves and non-sensitive tasks outside.
The main goal of SOTERIA, besides providing alternatives for state-of-the-art solutions is to improve the security of running these workloads in the real world.

**Warning 1**: This repository is being rebuilt (v2.0) for current Intel SGX (DCAP) and Gramine. See [Status and roadmap](#status-and-roadmap) and [docs/REBUILD_PLAN.md](docs/REBUILD_PLAN.md).

**Warning 2**: This is an academic proof-of-concept prototype and has not received careful code review. This implementation is NOT ready for production use.

**Warning 3**: The original Graphene v1.0 / Ubuntu 18.04 deployment files are kept in `legacy/` for reference only. The current Gramine setup is in `gramine/`.

### Status and roadmap

The Scala library in `src/` stores datasets as encrypted Parquet (AES-GCM) and places computation with Spark stage-level scheduling: stages that touch raw data run on executors holding the `enclave` resource, and in SML-2 only per-partition statistics are combined on untrusted executors. This placement is verified on a real standalone cluster (`scripts/local-cluster/run.sh`). **The enclave itself is not in place yet**: until phase 3, the `enclave` resource is advertised by a plain worker, not by one running inside Gramine-SGX. The v2.0 rebuild proceeds in phases:

| Phase | Scope | Status |
|---|---|---|
| 0 | Builds on Spark 3.5 / Java 17, honest APIs, unit tests, CI | done |
| 1 | Encrypted storage: Parquet modular encryption (AES-GCM) with a SOTERIA KMS client | done |
| 2 | Real computation partitioning (SML-1 / SML-2) via Spark stage-level scheduling, plus a leakage auditor | done |
| 3 | Gramine (>= 1.8) manifests, SGX2 + DCAP attestation, RA-TLS key provisioning | planned |
| 4 | Reproduce the paper's evaluation (ALS, Bayes, GBT, K-Means, LDA, Linear, LR, PCA) vs. vanilla Spark | planned |

### Build and test

Requires Java 17 and sbt.

```bash
sbt test                                          # unit tests on local Spark
sbt "runMain examples.SoteriaExamples"           # runs on local[*]
sbt assembly                                      # fat jar (Spark is "provided")
scripts/local-cluster/run.sh                      # SML-2 placement check on a real standalone cluster
```

### SGX machine preflight (phase 3, step 1)

Before the Gramine bring-up, run this on the SGX machine (Rocky/RHEL 9.4+ or Ubuntu 22.04/24.04; no root needed):

```bash
scripts/check_sgx.sh
```

It checks the OS and kernel, the SGX CPU flags and device nodes, the Intel PSW/DCAP packages and `aesmd`, PCCS reachability, Gramine (including SGX2/EDMM), Java 17 and SELinux, and reports OK/WARN/FAIL for each.

On Rocky Linux 9: in-kernel SGX needs 9.4 or later, Intel publishes a RHEL 9.4 RPM repository for the SGX PSW/DCAP, and Gramine's EL9 packages are marked experimental (building from source is the fallback).

### Computation partitioning (SML-1 / SML-2)

Every stage runs on the application's **default resource profile**, which requires the custom `enclave` resource. Only workers running inside an enclave advertise it (see `scripts/local-cluster/enclave-discovery.sh`), so any stage that is not explicitly placed elsewhere, including whole MLlib trainers, runs in an enclave.

In **SML-2** (`spark.soteria.mode=SML2`, the default), `SoteriaSession.statistic` is the only way out of the enclave. It folds each partition into a statistic inside the enclave, such as a gradient sum or per-cluster (sum, count), and combines those statistics on untrusted executors. Those executors hold no keys and never see records. A `LeakageAuditor` checks every untrusted placement and rejects any whose lineage reaches raw data through something other than a statistic. In **SML-1** everything stays in enclaves.

| Algorithm | SML-2 behaviour |
|---|---|
| Logistic regression, linear regression, K-Means, naive Bayes, PCA | partitioned: untrusted executors combine statistics |
| ALS, GBT, LDA | enclave-only (MLlib wrappers) |

The partitioned trainers match MLlib's results (see `SoteriaMLSuite`). Cluster settings for SML-2:

```properties
spark.executor.cores                                   1
spark.executor.resource.enclave.amount                 1
spark.task.resource.enclave.amount                     1
spark.dynamicAllocation.enabled                        true
spark.dynamicAllocation.shuffleTracking.enabled        true
# on enclave workers only:
spark.worker.resource.enclave.amount                   <slots>
spark.worker.resource.enclave.discoveryScript          /path/to/enclave-discovery.sh
```

With a `local[*]` master, placements are audited but not applied, because Spark has no stage-level scheduling in local mode.

### Encrypted datasets

Datasets are stored with [Parquet modular encryption](https://parquet.apache.org/docs/file-format/data-pages/encryption/): every page and the footer are encrypted and authenticated with AES-GCM, so tampered files fail to load. Data keys are wrapped with a SOTERIA master key that is resolved per process, never shipped through the Spark configuration:

1. `SOTERIA_MASTER_KEYS=id:base64key[,id2:base64key]` (environment), or
2. `SOTERIA_MASTER_KEYS_FILE=/path/to/keys` (same format, one per line), or
3. `SoteriaKeyStore.register(id, key)` in-process (local mode and tests).

In an SGX deployment (phase 3) these are filled in by Gramine secret provisioning after remote attestation, so only attested enclaves hold master keys.

Encrypt a dataset on the data owner's machine, before uploading it:

```bash
export SOTERIA_MASTER_KEYS="soteria-master:$(openssl rand -base64 16)"
spark-submit --class soteria.crypto.EncryptDataset soteria.jar data.csv data.enc --format csv
```

Then, in a job with the same key provisioned:

```scala
val session = SoteriaCore.createSession("train", SoteriaConfig(requireProvisionedKey = true))
val train = session.loadEncryptedDataset[ClassificationData]("data.enc")
session.saveEncrypted(results, "results.enc")
```

### Installation

Supported: Rocky/RHEL 9.4+ and Ubuntu 22.04/24.04. The scripts use `sudo` for package installation.

1. **Build tools** (OpenJDK 17, sbt, make, git):
   ```bash
   scripts/install_build_deps.sh
   sbt test
   scripts/local-cluster/run.sh
   ```
2. **Apache Spark** (the version in `build.sbt`, into `/opt/spark`):
   ```bash
   scripts/install_spark.sh
   ```
3. **Gramine** (works without SGX through `gramine-direct`):
   ```bash
   scripts/install_gramine.sh
   ```
4. **SGX machine check** (Intel SGX PSW/DCAP installation follows once SGX is enabled in the BIOS):
   ```bash
   scripts/check_sgx.sh
   ```
5. **Run Spark under Gramine**: see [`gramine/README.md`](gramine/README.md).

### Usage

SOTERIA supports several machine learning workflows. Here's how you can use it:

1. **Create a SOTERIA Session**:
   ```scala
   import soteria.core.SoteriaCore
   val session = SoteriaCore.createSession("SoteriaApp")
   ```

2. **Load Data**:
   ```scala
   val encryptedDataset = session.loadEncryptedDataset[TrainingData]("path/to/data")
   ```

3. **Train a Model**:
   ```scala
   import soteria.ml.SoteriaML._
   val logisticRegression = new SoteriaLogisticRegression(session)
   val model = logisticRegression.train(encryptedDataset)
   ```

4. **Perform Inference**:
   ```scala
   val prediction = model.predict(features)
   ```

5. **Close Session**:
   ```scala
   session.close()
   ```

### Full paper

For more information, please see:
IEEE Xplore: [Privacy-Preserving Machine Learning on Apache Spark](https://ieeexplore.ieee.org/document/10314994)

If you need to cite our work:
```
@article{brito2023soteria,
  title={Privacy-preserving machine learning on Apache Spark},
  author={Brito, Claudia V. and Ferreira, Pedro G. and Portela, Bernardo L. and Oliveira, Rui C. and Paulo, Joao T.},
  journal={IEEE Access},
  volume={11},
  pages={127907--127930},
  year={2023},
  publisher={IEEE},
  url={https://ieeexplore.ieee.org/document/10314994}
}
```


--------

## Dependencies

SOTERIA is implemented in Scala and runs on:
- OpenJDK 17 and Apache Spark 3.5
- Gramine (>= 1.6) for running Spark inside SGX enclaves
- Intel SGX PSW and DCAP (in-kernel SGX driver, Linux >= 5.11 or RHEL 9.4+)

The original prototype used Graphene v1.0, SGX SDK 2.8, Ubuntu 18.04 and Cloudera CDH 6.3.2; those files are in `legacy/`.

<!--
___
## Overview

### Machine Learning and Attacks
SOTERIA was built based on the current attacks to the machine learning pipeline as seen in the figure below. 
Specifically, we will consider Adversarial Attacks, Model Extraction, Model Inversion and Membership Inference, and Reconstruction Attacks. 

<p align="center">
    <img src="images/ml_pipeline_refactor-1.png" alt="SOTERIA Architecture" title="Machine Learning Pipeline and Attacks">
</p>

### Architecture

As depicted in Figure 2 by the gray boxes, a Spark cluster is composed of a Master and several Worker nodes.
The architecture of SOTERIA consists of two main designs, SOTERIA-B (baseline) and SOTERIA-P (computation partitioning). 

<p align="center">
    <img src="images/arch_soteria_poster-1.png" alt="SOTERIA Architecture" title="SOTERIA Architecture and Flow">
</p>

SOTERIA-B intends to run all the workloads inside the enclaves, with both master and worker nodes running inside the enclaves.

SOTERIA-P resorts to the partitioning of computation between what runs inside the enclaves and outside the enclaves. With this, a single worker node becomes a double worker node, i.e., two workers run on the node, with one running inside the enclaves and the other outside the enclave. This mechanism reduces the amount of trusted code base to be run inside the enclaves which intends to reduce the overhead imposed by large amounts of code running inside SGX.

<p align="center">
    <img src="images/spark-sml2-1.png" alt="SOTERIA Designs" title="SOTERIA Twofold Worker Design">
</p>

### Security Proofs

In [`proofs`](https://github.com/claudiavmbrito/Soteria/tree/main/proofs), you can find the security proofs of SOTERIA. We discuss the security protocol followed by SOTERIA and define it formally. 

It is divided into two main sections: Section A present the full proof of SOTERIA for all components and Section B depicts the ML attacks and in which circumstances SOTERIA is secure against each attack. 
___

## Getting Started

### Dependencies

SOTERIA is mainly written in Scala, JAVA and C and was built and tested with Intel's SGX SDK `2.6`, SGX Driver `1.8` and Gramine `1.0` (previously named Graphene-SGX).

### Apache Spark

To install Apache Spark to test the vanilla version, please run and see `build.sh` in [`scripts`](https://github.com/claudiavmbrito/Soteria/tree/main/scripts).

#### Data Encryption

For easy to use encryption, we implement an encryption mechanism based on AES-GCM 128. Such file is implemented inside of Apache Spark allowing its broad use outside SOTERIA.


### Intel SGX

To install SGX SDK and its Driver, please see `install_sgx.sh` and run:

```
bash ./install_sgx.sh
```

### Gramine 

- To use the previous and base code of Gramine used to develop SOTERIA, please refer to https://github.com/gramineproject/gramine/tree/v1.0.
- To use the updated version of Gramine, follow [Gramine](https://github.com/gramineproject/gramine) documentation. 
- The manifest files need to be carefully changed to work with the new versions of Gramine. 
---

### Cluster in Cloudera 

To install Cloudera version for which SOTERIA was tested, please see `install_cluster.sh` and run:

```
bash ./install_cluster.sh
```

Then, change the Manifest directories accordingly.

___
-->

## Contact

Please contact us at `claudia.v.brito@inesctec.pt` with any questions.
