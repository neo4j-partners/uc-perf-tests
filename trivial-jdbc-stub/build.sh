#!/usr/bin/env bash
# Build trivial-jdbc-stub.jar from src/. Requires a JDK (tested with JDK 25).
set -euo pipefail

cd "$(dirname "$0")"
rm -rf build
mkdir -p build/classes
javac --release 8 -d build/classes src/trivial/TrivialDriver.java
cp -R src/META-INF build/classes/
jar cf trivial-jdbc-stub.jar -C build/classes .
rm -rf build
echo "Built trivial-jdbc-stub.jar ($(wc -c < trivial-jdbc-stub.jar | tr -d ' ') bytes)"
