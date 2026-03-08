#!/bin/bash
cd "$(dirname "$0")"
mvn -q package -DskipTests
echo "Built: target/insert_engine.jar"