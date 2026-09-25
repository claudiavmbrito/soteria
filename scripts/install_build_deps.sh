#!/usr/bin/env bash
# Installs what is needed to build and test SOTERIA: OpenJDK 17, sbt, make,
# git and curl. Rocky/RHEL 9 (dnf) or Ubuntu 22.04/24.04 (apt). Idempotent.
#
# Usage: scripts/install_build_deps.sh
set -euo pipefail

SUDO=""; [[ $EUID -ne 0 ]] && SUDO="sudo"
. /etc/os-release

case "$ID" in
  rocky|rhel|almalinux|centos)
    $SUDO dnf install -y java-17-openjdk-devel make git curl tar
    if ! command -v sbt >/dev/null; then
      $SUDO curl -fsSLo /etc/yum.repos.d/sbt-rpm.repo https://www.scala-sbt.org/sbt-rpm.repo
      $SUDO dnf install -y sbt
    fi
    JAVA17=/usr/lib/jvm/java-17-openjdk
    ;;
  ubuntu|debian)
    $SUDO apt-get update
    $SUDO apt-get install -y openjdk-17-jdk-headless make git curl gnupg ca-certificates
    if ! command -v sbt >/dev/null; then
      echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | $SUDO tee /etc/apt/sources.list.d/sbt.list >/dev/null
      curl -fsSL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" \
        | $SUDO gpg --dearmor -o /etc/apt/trusted.gpg.d/sbt.gpg
      $SUDO apt-get update && $SUDO apt-get install -y sbt
    fi
    JAVA17=/usr/lib/jvm/java-17-openjdk-amd64
    ;;
  *) echo "unsupported distribution: $ID" >&2; exit 1 ;;
esac

# Make Java 17 the default if several JDKs are installed.
if [[ -x $JAVA17/bin/java ]] && command -v alternatives >/dev/null; then
  $SUDO alternatives --set java "$JAVA17/bin/java" 2>/dev/null || true
elif [[ -x $JAVA17/bin/java ]] && command -v update-alternatives >/dev/null; then
  $SUDO update-alternatives --set java "$JAVA17/bin/java" 2>/dev/null || true
fi

echo
java -version 2>&1 | grep ' version '
echo "sbt: $(command -v sbt)"
echo
echo "Next, from the repository root:"
echo "  sbt test                       # unit tests"
echo "  scripts/local-cluster/run.sh   # SML-2 placement on a standalone cluster"
