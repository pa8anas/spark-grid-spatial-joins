#!/usr/bin/env bash
set -euo pipefail

JAR="target/spark-grid-spatial-joins-1.0-SNAPSHOT.jar"
RAILS="hdfs:///user/hduser/input/RAILS.csv"
AREALM="hdfs:///user/hduser/input/AREALM.csv"
OUTPUT="hdfs:///user/hduser/output/resultsA"
EPSILON="0.012"
PARTITIONS="200"

spark-submit \
  --class org.example.Main \
  --master yarn \
  --deploy-mode client \
  --executor-memory 6g \
  --num-executors 4 \
  "$JAR" \
  A "$RAILS" "$AREALM" "$OUTPUT" "$EPSILON" "$PARTITIONS"
