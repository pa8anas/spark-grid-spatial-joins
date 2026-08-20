package org.example;

import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * Baseline implementation corresponding to the algorithm described in the
 * project report: both datasets are replicated to the 3x3 cell neighborhood
 * and duplicate candidate pairs are removed after distance filtering.
 */
public final class Main {

    private static final int DEFAULT_PARTITIONS = 200;

    private Main() {
    }

    public static void main(String[] args) {
        if (args.length < 5) {
            printUsage();
            System.exit(1);
        }

        String query = args[0].trim().toUpperCase();
        String railsPath = args[1];
        String arealmPath = args[2];
        String outputPath = args[3];
        double epsilon = Double.parseDouble(args[4]);

        int k = 0;
        int partitions = DEFAULT_PARTITIONS;

        if ("A".equals(query)) {
            if (args.length >= 6) {
                partitions = Integer.parseInt(args[5]);
            }
        } else if ("B".equals(query)) {
            if (args.length < 6) {
                throw new IllegalArgumentException("Query B requires k");
            }
            k = Integer.parseInt(args[5]);
            if (args.length >= 7) {
                partitions = Integer.parseInt(args[6]);
            }
        } else {
            throw new IllegalArgumentException("Unknown query: " + query + ". Use A or B.");
        }

        if (epsilon <= 0) {
            throw new IllegalArgumentException("epsilon must be > 0");
        }
        if (partitions <= 0) {
            throw new IllegalArgumentException("partitions must be > 0");
        }

        SparkConf conf = new SparkConf().setAppName("SparkGridSpatialJoins");

        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            JavaRDD<Point> rails = loadPoints(sc, railsPath);
            JavaRDD<Point> arealm = loadPoints(sc, arealmPath);

            if ("A".equals(query)) {
                runQueryA(sc, rails, arealm, outputPath, epsilon, partitions);
            } else {
                runQueryB(rails, arealm, outputPath, epsilon, k, partitions);
            }
        }
    }

    private static JavaRDD<Point> loadPoints(JavaSparkContext sc, String path) {
        return sc.textFile(path)
                .filter(line -> line != null && !line.trim().isEmpty())
                .map(Main::parsePoint)
                .filter(point -> point != null);
    }

    /**
     * Expected layout: id<TAB>x<TAB>y. Comma-separated input is accepted as a
     * convenience. If the TIGER-derived files use another column ordering,
     * adapt this parser accordingly.
     */
    private static Point parsePoint(String line) {
        try {
            String[] parts = line.contains("\t") ? line.split("\t") : line.split(",");
            if (parts.length < 3) {
                return null;
            }
            String id = parts[0].trim();
            double x = Double.parseDouble(parts[1].trim());
            double y = Double.parseDouble(parts[2].trim());
            return new Point(id, x, y);
        } catch (RuntimeException ex) {
            return null;
        }
    }

    private static JavaPairRDD<String, Point> replicateToNeighborhood(
            JavaRDD<Point> points,
            double gridSize,
            int partitions) {

        return points.flatMapToPair(point -> {
                    long cx = (long) Math.floor(point.getX() / gridSize);
                    long cy = (long) Math.floor(point.getY() / gridSize);
                    List<Tuple2<String, Point>> out = new ArrayList<>(9);

                    for (int dx = -1; dx <= 1; dx++) {
                        for (int dy = -1; dy <= 1; dy++) {
                            out.add(new Tuple2<>(cellKey(cx + dx, cy + dy), point));
                        }
                    }
                    return out.iterator();
                })
                .partitionBy(new HashPartitioner(partitions));
    }

    private static String cellKey(long x, long y) {
        return x + ":" + y;
    }

    private static JavaPairRDD<String, String> qualifyingPairs(
            JavaRDD<Point> rails,
            JavaRDD<Point> arealm,
            double epsilon,
            int partitions) {

        double gridSize = epsilon / Math.sqrt(2.0);

        JavaPairRDD<String, Point> rGrid = replicateToNeighborhood(rails, gridSize, partitions);
        JavaPairRDD<String, Point> sGrid = replicateToNeighborhood(arealm, gridSize, partitions);

        return rGrid.join(sGrid)
                .values()
                .filter(pair -> pair._1().distanceTo(pair._2()) <= epsilon)
                .mapToPair(pair -> new Tuple2<>(pair._1().getId(), pair._2().getId()))
                .distinct(partitions);
    }

    private static void runQueryA(
            JavaSparkContext sc,
            JavaRDD<Point> rails,
            JavaRDD<Point> arealm,
            String outputPath,
            double epsilon,
            int partitions) {

        long count = qualifyingPairs(rails, arealm, epsilon, partitions).count();
        sc.parallelize(List.of(Long.toString(count)), 1).saveAsTextFile(outputPath);
        System.out.println("Query A qualifying pairs: " + count);
    }

    private static void runQueryB(
            JavaRDD<Point> rails,
            JavaRDD<Point> arealm,
            String outputPath,
            double epsilon,
            int k,
            int partitions) {

        qualifyingPairs(rails, arealm, epsilon, partitions)
                .mapToPair(pair -> new Tuple2<>(pair._1(), 1L))
                .reduceByKey(new HashPartitioner(partitions), Long::sum)
                .filter(entry -> entry._2() > k)
                .map(entry -> entry._1() + "\t" + entry._2())
                .saveAsTextFile(outputPath);
    }

    private static void printUsage() {
        System.err.println("Usage:");
        System.err.println("  Query A: Main A <RAILS> <AREALM> <output> <epsilon> [partitions]");
        System.err.println("  Query B: Main B <RAILS> <AREALM> <output> <epsilon> <k> [partitions]");
    }
}
