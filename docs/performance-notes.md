# Performance Notes and Improvement Roadmap

The repository preserves the baseline algorithm described in the academic report. A later review identified several important optimization opportunities. They are documented here explicitly so the limitations of the baseline are clear and future versions can be evaluated systematically.

## 1. Reduce unnecessary replication

The baseline assigns every point to its own cell and all eight neighboring cells. This guarantees broad candidate coverage, but it multiplies the number of intermediate records and can significantly increase shuffle volume and distance comparisons.

A better design should derive the minimum required neighboring-cell assignment from the cell geometry and the distance threshold rather than applying universal 3x3 replication to every point.

## 2. Avoid global `distinct()` when possible

Because both sides are replicated, the same `(r, s)` pair may be generated from several cells. The baseline therefore applies `distinct()` after distance filtering.

In Spark, `distinct()` requires a shuffle and can be expensive at scale. A stronger design would generate each candidate pair exactly once, for example by assigning responsibility for a pair to a canonical grid cell.

## 3. Replicate only one dataset

The baseline replicates both `R` and `S`. An alternative strategy is to keep one dataset in its home cell and replicate only the other dataset to the required neighboring cells. This can reduce intermediate data volume substantially while preserving candidate coverage.

The best side to replicate depends on dataset size, density, and partition distribution.

## 4. Add systematic scalability experiments

The original experiments vary `epsilon` and `k`, but a stronger performance study should also vary Spark execution parameters.

Suggested experiments:

### Number of Spark partitions

```text
50, 100, 200, 400, 800
```

Record:

- total wall-clock time,
- shuffle read/write,
- task duration distribution,
- spill to memory/disk,
- candidate-pair count.

### Number of executors

For example:

```text
2, 4, 6, 8 executors
```

while keeping executor memory and cores controlled where possible.

This makes it possible to discuss speedup, scalability, and whether the job is CPU-, memory-, or shuffle-bound.

## 5. Investigate data skew

Uniform grid partitioning does not guarantee uniform computational load when points are spatially concentrated. Dense cells may dominate execution time.

Potential improvements include:

- adaptive grid sizing,
- skew-aware partitioning,
- salting overloaded keys,
- sampling-based partition design.

## 6. Compare against alternative spatial indexing approaches

Useful future baselines include:

- R-tree,
- quadtree,
- Apache Sedona spatial joins,
- optimized one-sided grid replication.

## Recommended evaluation plan

A future optimized version should preserve the current implementation as a **baseline** and compare each optimization under identical inputs and Spark resources. This provides a defensible before/after performance analysis instead of changing several factors at once.
