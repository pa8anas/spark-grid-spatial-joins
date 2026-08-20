# Original Source Files

This directory organizes the original Java source files uploaded from the coursework project while preserving their contents unchanged.

## Structure

```text
source/original/
├── local-prototype/
│   ├── Main.java
│   ├── GeoQueryUtils.java
│   ├── MyPoint.java
│   └── README.md
├── grid-experiments/
│   ├── MainA.java
│   ├── MainB.java
│   └── README.md
├── Main.java
├── MainB.java
└── README.md
```

### Local prototype

The `local-prototype/` folder contains the earlier local implementation. It loads the RAILS and AREALM datasets, represents records with `MyPoint`, and evaluates Query A and Query B using Cartesian products through `GeoQueryUtils`.

### Grid-based experimental sources

The `grid-experiments/` folder contains the original grid-based Spark/YARN implementations used during experimentation. `MainA.java` corresponds to Query A and `MainB.java` to Query B. These files contain multiple commented development iterations, which are intentionally preserved as part of the project history.

The original root-level Java files have not been deleted or rewritten. The organized copies in this directory exist only to make the repository easier to navigate.

For execution commands and HDFS result handling, see [`docs/experiment-commands.md`](../../docs/experiment-commands.md).
