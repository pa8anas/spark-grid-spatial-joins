# Spark Grid-Based Spatial Joins

A distributed geospatial processing project that implements **grid-based spatial joins with Apache Spark** over the RAILS and AREALM datasets. The project studies two distance-based spatial queries and evaluates how the distance threshold affects result size and execution time.

## Overview

Spatial joins are computationally expensive because a naive implementation may compare every point in one dataset with every point in the other. This project reduces the search space by partitioning the geographic space into grid cells and evaluating candidate point pairs within shared cell assignments.

The implementation was developed as an academic project in the MSc program at the University of Piraeus.

## Queries

### Query A — Distance Spatial Join

Given point sets `R` and `S` and a distance threshold `epsilon`, find all pairs `(r, s)` such that:

```text
dist(r, s) <= epsilon
```

The output is the number of qualifying point pairs.

### Query B — Neighborhood Count

Given point sets `R` and `S`, a distance threshold `epsilon`, and an integer `k`, find every point `r` in `R` that has more than `k` points from `S` within distance `epsilon`.

The output contains:

```text
{ r.id, count }
```

where `count` is the number of qualifying neighbors from `S`.

## Grid-Based Approach

The geographic space is divided into equal-sized cells using:

```text
gridSize = epsilon / sqrt(2)
```

In the current academic implementation, points are assigned to their own grid cell and neighboring cells to address boundary effects. Spark key-value transformations, partitioning, joins/cogroups, distance filtering, and duplicate removal are then used to compute the final results.

## Datasets

The experiments use two real geospatial datasets derived from TIGER data:

| Dataset | Records |
| --- | ---: |
| RAILS (`R`) | 3,184,948 |
| AREALM (`S`) | 3,848,971 |

The datasets are not stored in this repository because of their size.

## Experimental Setup

The reported experiments use Apache Spark on YARN with grid-based partitioning and **200 Spark partitions**. The example Spark submission configuration uses **4 executors** with **6 GB executor memory**.

## Query A Results

| epsilon | Number of pairs | Execution time (s) |
| ---: | ---: | ---: |
| 0.012 | 41,927,160 | 900 |
| 0.008 | 18,819,585 | 511 |
| 0.006 | 10,602,167 | 469 |
| 0.003 | 2,739,782 | 317 |

As `epsilon` increases, more candidate pairs satisfy the spatial condition and execution time also increases.

## Query B Results

| epsilon | k | Number of records r | Execution time (s) |
| ---: | ---: | ---: | ---: |
| 0.006 | 200 | 1,282 | 469 |
| 0.006 | 100 | 9,696 | 525 |
| 0.003 | 200 | 115 | 349 |
| 0.003 | 100 | 515 | 342 |

A larger `epsilon` generally produces more neighbors, while a larger `k` makes the query criterion stricter.

## Repository Structure

```text
spark-grid-spatial-joins/
├── data/
│   └── README.md
├── docs/
│   ├── algorithm.md
│   └── performance-notes.md
├── report/
│   ├── spatial-joins-spark-report-en.tex
│   └── spatial-joins-spark-report-el.tex
├── results/
│   ├── query_a_results.csv
│   └── query_b_results.csv
├── scripts/
│   └── run-query-a.sh
├── src/
│   └── README.md
├── .gitignore
├── LICENSE
└── README.md
```

## Running the Spark Job

An example command from the project experiments is available in [`scripts/run-query-a.sh`](scripts/run-query-a.sh).

The input and output paths must be adapted to your HDFS environment.

## Performance Considerations

The current implementation prioritizes correctness and a straightforward grid-based design. Important optimization opportunities include reducing unnecessary replication, avoiding expensive global deduplication when possible, replicating only one side of the join, and performing systematic scalability experiments over the number of Spark partitions and executors.

See [`docs/performance-notes.md`](docs/performance-notes.md) for details.

## Reports

The repository includes both English and Greek LaTeX versions of the project report. Compiled PDF versions can be added under `report/` as:

```text
spatial-joins-spark-report-en.pdf
spatial-joins-spark-report-el.pdf
```

## Technologies

`Apache Spark` · `Hadoop` · `HDFS` · `YARN` · `Distributed Computing` · `Spatial Joins` · `Grid-Based Indexing` · `Geospatial Data`

## License

This project is released under the [MIT License](LICENSE).
