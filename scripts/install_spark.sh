#!/usr/bin/env bash
# Installs the Apache Spark binary distribution matching build.sbt into
# /opt/spark-<version> and points /opt/spark at it. The Gramine manifests
# run Spark's Master and Workers from there.
#
# Usage: scripts/install_spark.sh [prefix]   (default prefix: /opt)
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PREFIX="${1:-/opt}"
VERSION="$(sed -nE 's/^val sparkVersion = "([^"]+)".*/\1/p' "$ROOT/build.sbt")"
[[ -n $VERSION ]] || { echo "could not read sparkVersion from build.sbt" >&2; exit 1; }
NAME="spark-$VERSION-bin-hadoop3"
URL="https://archive.apache.org/dist/spark/spark-$VERSION/$NAME.tgz"
SUDO=""; [[ -w $PREFIX ]] || SUDO="sudo"

if [[ -x "$PREFIX/spark-$VERSION/bin/spark-submit" ]]; then
  echo "Spark $VERSION already installed in $PREFIX/spark-$VERSION"
else
  tmp="$(mktemp -d)"; trap 'rm -rf "$tmp"' EXIT
  echo "downloading $URL"
  curl -fL --retry 3 --progress-bar -o "$tmp/$NAME.tgz" "$URL"
  curl -fsSL -o "$tmp/$NAME.tgz.sha512" "$URL.sha512"
  # Apache publishes either "<hash>  <file>" or "<file>: <HEX HEX ...>".
  expected="$(sed "s/$NAME.tgz//" "$tmp/$NAME.tgz.sha512" | tr -cd '0-9a-fA-F' | tr 'A-F' 'a-f')"
  [[ ${#expected} -eq 128 ]] || { echo "could not parse $URL.sha512" >&2; exit 1; }
  actual="$(sha512sum "$tmp/$NAME.tgz" | cut -d' ' -f1)"
  [[ "$expected" == "$actual" ]] || { echo "SHA-512 mismatch for $NAME.tgz" >&2; exit 1; }
  $SUDO tar -xzf "$tmp/$NAME.tgz" -C "$PREFIX"
  $SUDO mv "$PREFIX/$NAME" "$PREFIX/spark-$VERSION"
fi
$SUDO ln -sfn "$PREFIX/spark-$VERSION" "$PREFIX/spark"
echo "SPARK_HOME=$PREFIX/spark ($("$PREFIX/spark/bin/spark-submit" --version 2>&1 | grep -m1 -o 'version [0-9.]*' || echo "version $VERSION"))"
