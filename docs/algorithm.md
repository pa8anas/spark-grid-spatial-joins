# Algorithm

## Problem

Let `R` and `S` be two sets of geospatial points. Each point contains an identifier and two coordinates `(x, y)`.

The project implements two queries:

- **Query A:** count all pairs `(r, s)` such that `dist(r, s) <= epsilon`.
- **Query B:** return every `r` having more than `k` points `s` within distance `epsilon`.

## Baseline grid strategy

The report defines the grid cell size as:

```text
gridSize = epsilon / sqrt(2)
```

For each point:

1. Compute the point's base grid cell.
2. Replicate the point to the base cell and its eight neighbors.
3. Key each replicated record by the grid-cell identifier.
4. Partition the keyed RDDs with a `HashPartitioner`.
5. Join the keyed `R` and `S` RDDs.
6. Compute Euclidean distance only for generated candidate pairs.
7. Retain pairs satisfying `dist(r, s) <= epsilon`.
8. Remove duplicate `(r.id, s.id)` pairs produced by multi-cell replication.

## Query A

After duplicate removal, Query A counts the remaining point pairs.

```text
R -> grid assignment --\
                      join -> distance filter -> distinct -> count
S -> grid assignment --/
```

## Query B

Query B starts from the same qualifying pair set, groups the results by `r.id`, counts the number of neighbors from `S`, and filters by `count > k`.

```text
qualifying pairs
      |
      v
(r.id, s.id)
      |
      v
(r.id, 1)
      |
      v
reduceByKey(sum)
      |
      v
count > k
```

## Complexity intuition

A brute-force join requires up to `|R| x |S|` distance comparisons. Grid partitioning attempts to reduce this by producing candidate pairs only from spatially relevant cells. The actual efficiency depends heavily on:

- `epsilon`,
- spatial data density and skew,
- grid-cell size,
- replication strategy,
- number of Spark partitions,
- shuffle volume,
- duplicate-removal cost.

The implementation under `src/` intentionally mirrors the baseline approach described in the academic report so that the reported design is reproducible from the repository.
