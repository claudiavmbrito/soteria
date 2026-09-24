# SOTERIA: Privacy-Preserving Machine Learning for Apache Spark

**SOTERIA** is a privacy-preserving machine learning solution developed on top of [Apache Spark](https://github.com/apache/spark) using Intel SGX for secure computation. It employs computation partitioning to perform sensitive computing tasks within secure enclaves and non-sensitive tasks outside.
The main goal of SOTERIA, besides providing alternatives for state-of-the-art solutions is to improve the security of running these workloads in the real world.

**Warning 1**: This repository is being rebuilt (v2.0) for current Intel SGX (DCAP) and Gramine. See [Status and roadmap](#status-and-roadmap) and [docs/REBUILD_PLAN.md](docs/REBUILD_PLAN.md).

**Warning 2**: This is an academic proof-of-concept prototype and has not received careful code review. This implementation is NOT ready for production use.

**Warning 3**: The deployment files in `graphene-sgx-spark/` target Graphene v1.0 and are kept for reference. Newer Gramine versions need the new manifests (roadmap phase 3).

### Status and roadmap

The Scala library in `src/` stores datasets as encrypted Parquet (AES-GCM) and classifies each operation as sensitive (enclave) or non-sensitive (untrusted), but **it does not yet run anything in an enclave**: zone decisions are logged, not enforced. The v2.0 rebuild proceeds in phases:

| Phase | Scope | Status |
|---|---|---|
| 0 | Builds on Spark 3.5 / Java 17, honest APIs, unit tests, CI | done |
| 1 | Encrypted storage: Parquet modular encryption (AES-GCM) with a SOTERIA KMS client | done |
| 2 | Real computation partitioning (SML-1 / SML-2) via Spark stage-level scheduling, plus a leakage auditor | planned |
| 3 | Gramine (>= 1.8) manifests, SGX2 + DCAP attestation, RA-TLS key provisioning | planned |
| 4 | Reproduce the paper's evaluation (ALS, Bayes, GBT, K-Means, LDA, Linear, LR, PCA) vs. vanilla Spark | planned |

### Build and test

Requires Java 17 and sbt.

```bash
sbt test                                          # unit tests on local Spark
sbt "runMain examples.SoteriaExamples"           # runs on local[*]
sbt assembly                                      # fat jar (Spark is "provided")
```

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

Follow these steps to set up SOTERIA:

1. **Install SGX Drivers and SDK**: Run the `install_sgx.sh` script.
   ```bash
   sudo bash scripts/install_sgx.sh
   ```

2. **Set up Cloudera Cluster**: SOTERIA was tested with Cloudera. Run the `install_cluster.sh` script.
   ```bash
   sudo bash scripts/install_cluster.sh
   ```

3. **Build Spark with Graphene**: Navigate to the `graphene-sgx-spark/spark` directory and use `make`.
   ```bash
   cd graphene-sgx-spark/spark
   make
   ```

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

SOTERIA is implemented in Scala, Java, and C. It requires:
- Intel SGX SDK
- Apache Spark
- Gramine (formerly Graphene)

Tested OS: Ubuntu 18.04 SP2

**NOTE**: This has been tested in Ubuntu 18.04, it has not been tested in newer OS versions.

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
