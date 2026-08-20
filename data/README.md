# Data

This project uses the **RAILS** and **AREALM** geospatial datasets, derived from TIGER data.

The experimental report contains the following dataset sizes:

| Dataset | Records |
| --- | ---: |
| RAILS | 3,184,948 |
| AREALM | 3,848,971 |

The full datasets are intentionally not committed to this repository because of their size.

## Expected input

The baseline Java implementation currently expects each input row to expose at least three fields:

```text
id<TAB>x<TAB>y
```

Comma-separated rows are also accepted by the parser. If your local TIGER-derived files use another column ordering, update `parsePoint()` in `src/main/java/org/example/Main.java`.

## HDFS example

```bash
hdfs dfs -mkdir -p /user/hduser/input
hdfs dfs -put RAILS.csv /user/hduser/input/
hdfs dfs -put AREALM.csv /user/hduser/input/
```

Do not commit large raw datasets or generated Spark output folders to Git.
