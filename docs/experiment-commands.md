# Spark Experiment Commands

This document records the Spark commands used to execute **Query A** and **Query B** in the experiments, together with the HDFS commands used to inspect and export the results.

## Query A

### 1. Run with `epsilon = 0.008` in cluster mode

```bash
spark-submit \
  --class org.example.Main \
  --master yarn \
  --deploy-mode cluster \
  --executor-memory 6g \
  --num-executors 4 \
  --conf spark.network.timeout=1000s \
  --conf spark.hadoop.mapred.output.committer.algorithm.version=2 \
  --conf spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2 \
  --conf spark.hadoop.validateOutputSpecs=false \
  --conf spark.hadoop.dfs.replication=1 \
  spark_project-1.0-SNAPSHOT.jar \
  hdfs:///user/hduser/input/RAILS.csv \
  hdfs:///user/hduser/input/AREALM.csv \
  hdfs:///user/hduser/output/resultsA/ \
  0.008
```

### 2. Run with `epsilon = 0.012` in client mode

```bash
spark-submit \
  --class org.example.Main \
  --master yarn \
  --deploy-mode client \
  --executor-memory 6g \
  --num-executors 4 \
  --conf spark.network.timeout=1000s \
  --conf spark.hadoop.mapred.output.committer.algorithm.version=2 \
  --conf spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2 \
  --conf spark.hadoop.validateOutputSpecs=false \
  --conf spark.hadoop.dfs.replication=1 \
  spark_project-1.0-SNAPSHOT.jar \
  hdfs:///user/hduser/input/RAILS.csv \
  hdfs:///user/hduser/input/AREALM.csv \
  hdfs:///user/hduser/output/resultsA/ \
  0.012
```

### 3. Check the result-count directory in HDFS

```bash
hdfs dfs -ls /user/hduser/output/resultsA/_count/
```

### 4. Display the total number of matching pairs

```bash
hdfs dfs -cat /user/hduser/output/resultsA/_count/part-*
```

### 5. Merge all count result parts into a local file

```bash
hdfs dfs -getmerge /user/hduser/output/resultsA/_count/ resultsA.txt
```

### 6. Display the final local result

```bash
cat resultsA.txt
```

### Query A output locations

- Detailed pair results:

```text
/user/hduser/output/resultsA/
```

- Pair count:

```text
/user/hduser/output/resultsA/_count/
```

---

## Query B

### 1. Run with `epsilon = 0.006`, `k = 200` in cluster mode

```bash
spark-submit \
  --class org.example.MainB \
  --master yarn \
  --deploy-mode cluster \
  --executor-memory 6g \
  --num-executors 4 \
  --conf spark.network.timeout=1000s \
  --conf spark.hadoop.mapred.output.committer.algorithm.version=2 \
  --conf spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2 \
  --conf spark.hadoop.validateOutputSpecs=false \
  --conf spark.hadoop.dfs.replication=1 \
  spark_project-1.0-SNAPSHOT.jar \
  0.006 200 \
  hdfs:///user/hduser/input/RAILS.csv \
  hdfs:///user/hduser/input/AREALM.csv \
  hdfs:///user/hduser/output/resultsB
```

### 2. Run with `epsilon = 0.003`, `k = 100` in client mode

```bash
spark-submit \
  --class org.example.MainB \
  --master yarn \
  --deploy-mode client \
  --executor-memory 6g \
  --num-executors 4 \
  --conf spark.network.timeout=1000s \
  --conf spark.hadoop.mapred.output.committer.algorithm.version=2 \
  --conf spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2 \
  --conf spark.hadoop.validateOutputSpecs=false \
  --conf spark.hadoop.dfs.replication=1 \
  spark_project-1.0-SNAPSHOT.jar \
  0.003 100 \
  hdfs:///user/hduser/input/RAILS.csv \
  hdfs:///user/hduser/input/AREALM.csv \
  hdfs:///user/hduser/output/resultsB
```

### 3. Count the number of result records directly from HDFS

```bash
hdfs dfs -cat /user/hduser/output/resultsB_0.006_200/part-* | wc -l
```

### 4. Merge the HDFS output into a local file

```bash
hdfs dfs -getmerge /user/hduser/output/resultsB_0.006_200/* resultsB.txt
```

### 5. Inspect the local output

```bash
wc -l resultsB.txt
more resultsB.txt
cat resultsB.txt
```

### Query B output location

Each run stores its final results under:

```text
/user/hduser/output/resultsB_<epsilon>_<k>/
```

---

## Experimental Result Files

The original project archive also contains `.txt` files with the raw experiment outputs for the tested parameter combinations. These files can be stored under:

```text
results/raw/
```

Suggested naming convention:

```text
query-a-eps-0.008.txt
query-a-eps-0.012.txt
query-b-eps-0.006-k-200.txt
query-b-eps-0.003-k-100.txt
```

These raw files complement the summarized CSV result tables already included in the repository.
