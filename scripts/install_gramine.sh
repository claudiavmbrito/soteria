#!/usr/bin/env bash
# Installs Gramine from its package repository and creates an enclave signing
# key. Works without SGX hardware: gramine-direct runs the same manifests
# outside an enclave, which is how the bring-up starts.
#
# Rocky/RHEL 9: Gramine's EL9 packages are marked experimental (EL8 is the
# validated target). If they misbehave, build from source:
# https://gramine.readthedocs.io/en/stable/devel/building.html
#
# Usage: scripts/install_gramine.sh
set -euo pipefail

SUDO=""; [[ $EUID -ne 0 ]] && SUDO="sudo"
. /etc/os-release

case "$ID" in
  rocky|rhel|almalinux|centos)
    echo "note: Gramine support for EL${VERSION_ID%%.*} is experimental"
    $SUDO curl -fsSLo /etc/yum.repos.d/gramine.repo https://packages.gramineproject.io/rpm/gramine.repo
    $SUDO dnf install -y gramine
    ;;
  ubuntu|debian)
    $SUDO curl -fsSLo /usr/share/keyrings/gramine-keyring.gpg \
      "https://packages.gramineproject.io/gramine-keyring-$VERSION_CODENAME.gpg"
    echo "deb [arch=amd64 signed-by=/usr/share/keyrings/gramine-keyring.gpg] https://packages.gramineproject.io/ $VERSION_CODENAME main" \
      | $SUDO tee /etc/apt/sources.list.d/gramine.list >/dev/null
    $SUDO apt-get update && $SUDO apt-get install -y gramine
    ;;
  *) echo "unsupported distribution: $ID" >&2; exit 1 ;;
esac

key="${XDG_CONFIG_HOME:-$HOME/.config}/gramine/enclave-key.pem"
if [[ ! -f $key ]]; then
  gramine-sgx-gen-private-key
  echo "created enclave signing key $key (keep it private; it defines MRSIGNER)"
fi

echo
gramine-manifest --version 2>/dev/null | head -1 || true
command -v gramine-direct gramine-sgx
command -v is-sgx-available >/dev/null && { is-sgx-available || true; }
