#!/usr/bin/env bash
# Spark resource discovery script for the SOTERIA enclave resource.
# A worker advertises "enclave" only when it runs inside a Gramine SGX
# enclave (/dev/attestation exists). SOTERIA_FAKE_ENCLAVE=1 forces it, for
# local testing without SGX.
if [[ -e /dev/attestation || "${SOTERIA_FAKE_ENCLAVE:-0}" == "1" ]]; then
  echo '{"name": "enclave", "addresses": ["0", "1"]}'
else
  echo '{"name": "enclave", "addresses": []}'
fi
