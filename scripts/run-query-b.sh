#!/usr/bin/env bash
set -euo pipefail

JAR="target/spark-grid-spatial-joins-1.0-SNAPSHOT.jar"
RAILS="hdfs:///user/hduser/input/RAILS.csv"
AREALM="hdfs:///user/hduser/input/AREALM.csv"
OUTPUT="hdfs:///user/hduser/output/resultsB"
EPSILON="0.006"
K="200"
PARTITIONS="200"

spark-submit \
  --class org.example.Main \
  --master yarn \
  --deploy-mode client \
  --executor-memory 6g \
  --num-executors 4 \
  "$JAR" \
  B "$RAILS" "$AREALM" "$OUTPUT" "$EPSILON" "$K" "$PARTITIONS"
