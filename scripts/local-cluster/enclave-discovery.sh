#!/usr/bin/env bash
# Spark resource discovery script for the SOTERIA "enclave" resource.
#
# A Worker advertises the resource only when the executors it launches run
# inside enclaves:
#   - /dev/attestation exists (this process itself runs in a Gramine SGX enclave);
#   - SOTERIA_ENCLAVE_RUNNER=gramine-sgx and /dev/sgx_enclave exists (a native
#     Worker whose executors are launched through gramine-sgx; see gramine/);
#   - SOTERIA_FAKE_ENCLAVE=1 (testing without SGX: local-cluster, gramine-direct).
# SOTERIA_ENCLAVE_SLOTS sets how many enclave slots are advertised (default 2).
slots="${SOTERIA_ENCLAVE_SLOTS:-2}"
if [[ -e /dev/attestation || "${SOTERIA_FAKE_ENCLAVE:-0}" == "1" ||
      ( "${SOTERIA_ENCLAVE_RUNNER:-}" == "gramine-sgx" && -e /dev/sgx_enclave ) ]]; then
  addresses=$(seq -s '", "' 0 $((slots - 1)))
  echo "{\"name\": \"enclave\", \"addresses\": [\"$addresses\"]}"
else
  echo '{"name": "enclave", "addresses": []}'
fi
