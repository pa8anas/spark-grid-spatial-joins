# Experimental Results

This directory contains the measurements reported in the project report.

- `query_a_results.csv` records the number of qualifying spatial pairs and total execution time for different values of `epsilon`.
- `query_b_results.csv` records the number of qualifying `R` records and execution time for different `(epsilon, k)` combinations.

The experiments reported in the paper used 200 Spark partitions. The Spark submission example used YARN with 4 executors and 6 GB executor memory.

These measurements describe the baseline implementation. They should not be interpreted as a complete scalability study because the original experiments did not systematically vary the number of Spark partitions or executors. See `../docs/performance-notes.md` for the proposed evaluation and optimization roadmap.
