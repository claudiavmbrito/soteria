#!/bin/sh

# For vanilla tests
sudo apt-get install maven git scala -y
export MAVEN_OPTS="-Xmx2g -XX:ReservedCodeCacheSize=512m"

echo "Building and compiling spark needs javac - Oracle 1.8_211"

echo "Go to https://github.com/apache/spark/tree/v2.3.4-rc1 and clone it or follow the next steps"
wget https://github.com/apache/spark/archive/refs/tags/v2.3.4.zip
unzip -qq v2.3.4.zip

