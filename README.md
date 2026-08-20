# Spark Grid-Based Spatial Joins

A distributed geospatial processing project for **grid-based spatial joins with Apache Spark** over the RAILS and AREALM datasets. The project studies two distance-based spatial queries and evaluates how the distance threshold affects result size and execution time.

## Overview

Spatial joins are computationally expensive because a naive implementation may compare every point in one dataset with every point in the other. This project reduces the search space by partitioning geographic space into grid cells and evaluating candidate point pairs associated with common grid keys.

The repository is based on an academic project developed at the University of Piraeus. The original Java files uploaded for the project are preserved unchanged at the repository root as `Main.java` and `MainB.java`. For easier navigation, identical copies are also grouped under [`source/original/`](source/original/).

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

The report defines the cell size as:

```text
gridSize = epsilon / sqrt(2)
```

The implementation assigns points to their own grid cell and the eight neighboring cells to address boundary effects. Spark key-value transformations, `HashPartitioner`, joins/cogroups, Euclidean-distance filtering, and duplicate removal are then used to compute the final results.

## Datasets

The experiments use two real geospatial datasets derived from TIGER data:

| Dataset | Records |
| --- | ---: |
| RAILS (`R`) | 3,184,948 |
| AREALM (`S`) | 3,848,971 |

The full datasets are not stored in this repository because of their size. See [`data/README.md`](data/README.md).

## Experimental Setup

The reported experiments use Apache Spark on YARN with **200 Spark partitions**. The example Spark submission configuration uses **4 executors** with **6 GB executor memory**.

## Query A Results

| epsilon | Number of pairs | Execution time (s) |
| ---: | ---: | ---: |
| 0.012 | 41,927,160 | 900 |
| 0.008 | 18,819,585 | 511 |
| 0.006 | 10,602,167 | 469 |
| 0.003 | 2,739,782 | 317 |

As `epsilon` increases, the number of qualifying pairs and the execution time increase substantially.

## Query B Results

| epsilon | k | Number of records r | Execution time (s) |
| ---: | ---: | ---: | ---: |
| 0.006 | 200 | 1,282 | 469 |
| 0.006 | 100 | 9,696 | 525 |
| 0.003 | 200 | 115 | 349 |
| 0.003 | 100 | 515 | 342 |

A larger `epsilon` generally produces more neighbors, while a larger `k` makes the query criterion stricter.

Raw result tables are available under [`results/`](results/).

## Repository Structure

```text
spark-grid-spatial-joins/
├── Main.java                     # original uploaded source
├── MainB.java                    # original uploaded source
├── source/
│   └── original/
│       ├── README.md
│       ├── Main.java             # mirrored copy for organization
│       └── MainB.java            # mirrored copy for organization
├── data/
│   └── README.md
├── docs/
│   ├── algorithm.md
│   └── performance-notes.md
├── report/
│   ├── README.md
│   ├── spatial-joins-spark-report-en.pdf
│   └── spatial-joins-spark-report-el.pdf
├── results/
│   ├── README.md
│   ├── query_a_results.csv
│   └── query_b_results.csv
├── .gitignore
├── CITATION.cff
├── LICENSE
├── pom.xml
└── README.md
```

## Source Code Notes

The original uploaded Java files contain multiple implementation iterations, including commented-out versions and later active versions. They are intentionally preserved rather than rewritten, so the repository reflects the actual coursework development history.

For a cleaner production-style version, the next step would be to extract the final active implementations into a standard Maven layout such as `src/main/java/org/example/`, while keeping the original files archived unchanged under `source/original/`.

## Performance Considerations

The current design has several important optimization opportunities: reducing unnecessary 3x3 replication, avoiding an expensive global `distinct()` when possible, replicating only one side of the join, investigating spatial skew, and performing systematic scalability experiments over Spark partitions and executors.

These limitations and a concrete improvement plan are documented in [`docs/performance-notes.md`](docs/performance-notes.md).

## Report

The compiled reports are available in [`report/`](report/):

- [`spatial-joins-spark-report-en.pdf`](report/spatial-joins-spark-report-en.pdf) — English version
- [`spatial-joins-spark-report-el.pdf`](report/spatial-joins-spark-report-el.pdf) — Greek version

## Technologies

`Apache Spark` · `Java` · `Maven` · `Hadoop` · `HDFS` · `YARN` · `Distributed Computing` · `Spatial Joins` · `Grid-Based Indexing` · `Geospatial Data`

## Citation

Citation metadata is included in [`CITATION.cff`](CITATION.cff).

## License

This project is released under the [MIT License](LICENSE).
