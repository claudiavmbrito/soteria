# SOTERIA benchmarks (phase 4)

Reproduces the paper's comparison for its eight workloads: ALS, naive Bayes,
GBT, K-Means, LDA, linear regression, logistic regression and PCA, on
vanilla Spark and on SOTERIA in SML-1 and SML-2.

| Mode | Data | Trainers | Where it runs |
|---|---|---|---|
| vanilla | plain Parquet | MLlib | native Master, Worker, executors and driver |
| sml1 | encrypted Parquet | SOTERIA | driver and executors in Gramine; statistics combined in enclaves too |
| sml2 | encrypted Parquet | SOTERIA | as SML-1, but statistic combines on untrusted native executors |

Both sides use the same generated data, input splits (`PARTITIONS`) and
hyperparameters (`soteria.bench.Bench`).
Every dataset is deterministic (seed and row id), is generated once per scale
under `gramine/work/bench/data/`, and is reused by later runs.

## Running (on the machine of `gramine/README.md`, steps 1–4 passed)

```bash
cd gramine
# The cluster of step 3 (master and both workers) must be running for sml1 and sml2.
make bench MODE=sml2                    # SML-2, driver in an enclave
make bench MODE=sml1                    # SML-1
make bench-vanilla                      # baseline: its own native mini-cluster on port 7078
make bench-plot                         # plots + summary.md in work/bench/plots/
```

Variables (defaults in brackets): `ALGO` [all] or a list such as `lr,kmeans`;
`SCALE` [0.1]; `REPS` [3] measured repetitions after `WARMUP` [1] warm-up;
`PARTITIONS` [8]; `WORKER_CORES` [2] executors per mode. Use the same `SCALE`
and `WORKER_CORES` for all three modes. With `SGX=1`, `make bench` runs under
`gramine-sgx` and its rows are labelled with that runner.

At scale 1.0 the datasets are:

| Workload | Algorithms | Size at scale 1.0 |
|---|---|---|
| classification | lr | 200k rows × 20 features |
| counts | bayes | 200k rows × 20 term counts |
| regression | linear, gbt | 200k rows × 20 features |
| clustering | kmeans | 200k rows × 10 features, 10 clusters |
| pca | pca | 100k rows × 50 features, top 10 components |
| ratings | als | 200k ratings, 30000 users × 40000 items, rank 10 |
| documents | lda | 10k documents, vocabulary 1000, 10 topics |

Start with the default `SCALE=0.1`, then increase it. Under SGX, raise
`ENCLAVE_SIZE` or lower `JVM_HEAP` if an enclave runs out of memory.

## Output

`gramine/work/bench/results.csv` gets one row per repetition:

`timestamp, runner, mode, algo, scale, rows, partitions, rep, warmup, train_s, metric, quality, enclave_tasks, untrusted_tasks, input_partitions`

- `train_s` is the time to train the model, including reading and decrypting the data.
- `quality`: training accuracy (lr, bayes), RMSE (linear, gbt, als), K-Means
  cost, explained variance (pca) or log perplexity (lda). It checks that
  SOTERIA trains the same models as vanilla Spark.
- `input_partitions` is the number of splits the data was read into; it is
  the same in every mode (`spark.sql.files.minPartitionNum` is fixed), so
  sampling algorithms (K-Means seeding, GBT binning, LDA) train identical
  models in SML-1 and SML-2. Vanilla reads the plain files, whose sizes
  differ from the encrypted ones; Spark orders files by size when it fills
  the splits, so its GBT and LDA results can differ slightly from SOTERIA's.
- `enclave_tasks` / `untrusted_tasks` count tasks by where they ran. In SML-2
  only the statistic combines of lr, linear, kmeans, bayes and pca run on
  untrusted executors; als, gbt and lda stay in enclaves (MLlib wrappers).
  Vanilla has no enclave, so all its tasks count as `untrusted_tasks`.

`make bench-plot` (needs `python3-matplotlib`) writes, per runner and scale,
the training time per algorithm (median with min–max), the overhead relative
to vanilla Spark, and `summary.md` with the same numbers and the model
quality. Warm-up rows are left out.

## Notes

- K-Means: MLlib starts from k-means|| and SOTERIA from k-means++ (seeded), so
  the two can end in different local optima and their costs can differ.
- The vanilla and SOTERIA sessions differ in more than the enclave: SOTERIA
  decrypts Parquet (AES-GCM) and uses its own partitioned trainers for five of
  the algorithms. Both are part of what the paper measures.
- Enclave statistics (EPC paging, `sgx.enable_stats`) are not collected yet;
  they will be added with the first SGX runs.
