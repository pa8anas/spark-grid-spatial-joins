/*
package org.example;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.HashPartitioner;
import scala.Tuple2;
import java.util.*;

public class MainB {
    public static void main(String[] args) {
        // 1. Διαβάζουμε παραμέτρους ε και k από γραμμή εντολών
        double epsilon = args.length > 0 ? Double.parseDouble(args[0]) : 0.5;
        int k = args.length > 1 ? Integer.parseInt(args[1]) : 10;

        // 2. Ρυθμίσεις Spark
        SparkConf conf = new SparkConf()
                .setAppName("SpatialQueryB")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .registerKryoClasses(new Class[]{Point.class});

        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            // 3. Φόρτωση δεδομένων
            JavaRDD<String> rData = sc.textFile("hdfs:///user/hduser/input/RAILS.csv");
            JavaRDD<String> sData = sc.textFile("hdfs:///user/hduser/input/AREALM.csv");

            // 4. Μετατροπή σε Point
            JavaRDD<Point> rPoints = rData.map(Point::fromCSV).filter(Objects::nonNull);
            JavaRDD<Point> sPoints = sData.map(Point::fromCSV).filter(Objects::nonNull);

            // 5. Υπολογισμός grid size
            double gridSize = epsilon / Math.sqrt(2);

            // 6. Δημιουργία grid cells
            JavaPairRDD<String, Point> rCells = createGridRDD(rPoints, gridSize)
                    .partitionBy(new HashPartitioner(200));

            JavaPairRDD<String, Point> sCells = createGridRDD(sPoints, gridSize)
                    .partitionBy(new HashPartitioner(200));

            // 7. Cogroup για ομαδοποίηση σημείων ανά κελί
            JavaPairRDD<String, Tuple2<Iterable<Point>, Iterable<Point>>> grouped =
                    rCells.cogroup(sCells);

            // 8. Υπολογισμός ζευγών και καταμέτρηση
            JavaPairRDD<String, Integer> counts = grouped.flatMapToPair(pair -> {
                        List<Tuple2<String, String>> candidatePairs = new ArrayList<>();
                        Iterable<Point> rPointsInCell = pair._2()._1();
                        Iterable<Point> sPointsInCell = pair._2()._2();

                        // Δημιουργία υποψήφιων ζευγών
                        for (Point r : rPointsInCell) {
                            for (Point s : sPointsInCell) {
                                if (r.distance(s) <= epsilon) {
                                    candidatePairs.add(new Tuple2<>(r.id, s.id));
                                }
                            }
                        }
                        return candidatePairs.iterator();
                    })
                    .distinct()  // Αποφυγή διπλοεγγραφών
                    .mapToPair(pair -> new Tuple2<>(pair._1(), 1))
                    .reduceByKey(Integer::sum)
                    .filter(pair -> pair._2() > k);  // Φιλτράρισμα με βάση το k

            // 9. Αποθήκευση αποτελεσμάτων
            JavaRDD<String> output = counts.map(pair -> "{" + pair._1 + ", " + pair._2 + "}");
            output.saveAsTextFile("hdfs:///user/hduser/output/resultsB_" + epsilon + "_" + k);

            // 10. Εκτύπωση στατιστικών
            System.out.println("R points with > " + k + " neighbors: " + counts.count());
        }
    }

    // Δημιουργεί RDD με grid cells και γειτονικές κυψέλες (ίδια με Query A)

    private static JavaPairRDD<String, Point> createGridRDD(JavaRDD<Point> points, double gridSize) {
        return points.flatMapToPair(point -> {
            List<Tuple2<String, Point>> cells = new ArrayList<>();
            int xCell = (int) Math.floor(point.x / gridSize);
            int yCell = (int) Math.floor(point.y / gridSize);

            for (int dx = -1; dx <= 1; dx++) {
                for (int dy = -1; dy <= 1; dy++) {
                    String cellKey = (xCell + dx) + "_" + (yCell + dy);
                    cells.add(new Tuple2<>(cellKey, point));
                }
            }
            return cells.iterator();
        });
    }

    // Κλάση Point (ίδια με Query A)

    public static class Point implements java.io.Serializable {
        public final String id;
        public final double x;
        public final double y;

        public Point(String id, double x, double y) {
            this.id = id;
            this.x = x;
            this.y = y;
        }

        public static Point fromCSV(String line) {
            String[] tokens = line.trim().split("\t");
            if (tokens.length < 3) {
                System.err.println("Invalid line: " + line);
                return null;
            }
            try {
                return new Point(
                        tokens[0].trim(),
                        Double.parseDouble(tokens[1].trim()),
                        Double.parseDouble(tokens[2].trim())
                );
            } catch (NumberFormatException e) {
                System.err.println("Number format error in line: " + line);
                return null;
            }
        }

        public double distance(Point other) {
            double dx = this.x - other.x;
            double dy = this.y - other.y;
            return Math.sqrt(dx*dx + dy*dy);
        }
    }
}
*/

/*
package org.example;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.HashPartitioner;
import scala.Tuple2;
import java.util.*;

public class MainB {
    public static void main(String[] args) {
        if (args.length < 5) {
            System.err.println("Usage: MainB <epsilon> <k> <inputR> <inputS> <output>");
            System.exit(1);
        }

        double epsilon = Double.parseDouble(args[0]);
        int k = Integer.parseInt(args[1]);
        String inputR = args[2]; // path για R
        String inputS = args[3]; // path για S
        String outputPath = args[4]; // φάκελος εξόδου

        // Ρυθμίσεις Spark
        SparkConf conf = new SparkConf()
                .setAppName("SpatialQueryB")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .registerKryoClasses(new Class[]{Point.class});

        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            // 1. Φόρτωση δεδομένων από paths
            JavaRDD<String> rData = sc.textFile(inputR);
            JavaRDD<String> sData = sc.textFile(inputS);

            // 2. Μετατροπή σε Point
            JavaRDD<Point> rPoints = rData.map(Point::fromCSV).filter(Objects::nonNull);
            JavaRDD<Point> sPoints = sData.map(Point::fromCSV).filter(Objects::nonNull);

            // 3. Υπολογισμός grid size
            double gridSize = epsilon / Math.sqrt(2);

            // 4. Δημιουργία grid cells
            JavaPairRDD<String, Point> rCells = createGridRDD(rPoints, gridSize)
                    .partitionBy(new HashPartitioner(200));

            JavaPairRDD<String, Point> sCells = createGridRDD(sPoints, gridSize)
                    .partitionBy(new HashPartitioner(200));

            // 5. Cogroup για ομαδοποίηση σημείων ανά κελί
            JavaPairRDD<String, Tuple2<Iterable<Point>, Iterable<Point>>> grouped =
                    rCells.cogroup(sCells);

            // 6. Υπολογισμός ζευγών και καταμέτρηση
            JavaPairRDD<String, Integer> counts = grouped.flatMapToPair(pair -> {
                        List<Tuple2<String, String>> candidatePairs = new ArrayList<>();
                        Iterable<Point> rPointsInCell = pair._2()._1();
                        Iterable<Point> sPointsInCell = pair._2()._2();

                        for (Point r : rPointsInCell) {
                            for (Point s : sPointsInCell) {
                                if (r.distance(s) <= epsilon) {
                                    candidatePairs.add(new Tuple2<>(r.id, s.id));
                                }
                            }
                        }
                        return candidatePairs.iterator();
                    })
                    .distinct()
                    .mapToPair(pair -> new Tuple2<>(pair._1, 1))
                    .reduceByKey(Integer::sum)
                    .filter(pair -> pair._2() > k);

            // 7. Αποθήκευση αποτελεσμάτων
            JavaRDD<String> output = counts.map(pair -> "{" + pair._1 + ", " + pair._2 + "}");
            output.saveAsTextFile(outputPath);

            // 8. Εκτύπωση στατιστικών

            long start = System.currentTimeMillis();
            long count = output.count();
            long end = System.currentTimeMillis();

            double seconds = (end - start) / 1000.0;
            System.out.println("Execution time: " + seconds + " sec");

            System.out.println("R points with > " + k + " neighbors: " + counts.count());
        }
    }

    // Grid partitioning με 3x3 γειτονικά κελιά
    private static JavaPairRDD<String, Point> createGridRDD(JavaRDD<Point> points, double gridSize) {
        return points.flatMapToPair(point -> {
            List<Tuple2<String, Point>> cells = new ArrayList<>();
            int xCell = (int) Math.floor(point.x / gridSize);
            int yCell = (int) Math.floor(point.y / gridSize);
            for (int dx = -1; dx <= 1; dx++) {
                for (int dy = -1; dy <= 1; dy++) {
                    String cellKey = (xCell + dx) + "_" + (yCell + dy);
                    cells.add(new Tuple2<>(cellKey, point));
                }
            }
            return cells.iterator();
        });
    }

    public static class Point implements java.io.Serializable {
        public final String id;
        public final double x;
        public final double y;

        public Point(String id, double x, double y) {
            this.id = id;
            this.x = x;
            this.y = y;
        }

        public static Point fromCSV(String line) {
            String[] tokens = line.trim().split("\t");
            if (tokens.length < 3) return null;
            try {
                return new Point(
                        tokens[0].trim(),
                        Double.parseDouble(tokens[1].trim()),
                        Double.parseDouble(tokens[2].trim())
                );
            } catch (NumberFormatException e) {
                return null;
            }
        }

        public double distance(Point other) {
            double dx = this.x - other.x;
            double dy = this.y - other.y;
            return Math.sqrt(dx*dx + dy*dy);
        }
    }
}
*/

package org.example;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.HashPartitioner;
import scala.Tuple2;
import java.util.*;
import java.io.PrintWriter;
import java.util.List;


public class MainB {
    public static void main(String[] args) {
        if (args.length < 5) {
            System.err.println("Usage: MainB <epsilon> <k> <inputR> <inputS> <output>");
            System.exit(1);
        }

        double epsilon = Double.parseDouble(args[0]);
        int k = Integer.parseInt(args[1]);
        String inputR = args[2]; // path για R
        String inputS = args[3]; // path για S
        String outputPath = args[4]; // φάκελος εξόδου

        // Ρυθμίσεις Spark
        SparkConf conf = new SparkConf()
                .setAppName("SpatialQueryB")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .registerKryoClasses(new Class[]{Point.class});

        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            // 1. Φόρτωση δεδομένων από paths
            JavaRDD<String> rData = sc.textFile(inputR);
            JavaRDD<String> sData = sc.textFile(inputS);

            // 2. Μετατροπή σε Point
            JavaRDD<Point> rPoints = rData.map(Point::fromCSV).filter(Objects::nonNull);
            JavaRDD<Point> sPoints = sData.map(Point::fromCSV).filter(Objects::nonNull);

            // 3. Υπολογισμός grid size
            double gridSize = epsilon / Math.sqrt(2);

            // 4. Δημιουργία grid cells
            JavaPairRDD<String, Point> rCells = createGridRDD(rPoints, gridSize)
                    .partitionBy(new HashPartitioner(200));

            JavaPairRDD<String, Point> sCells = createGridRDD(sPoints, gridSize)
                    .partitionBy(new HashPartitioner(200));

            // 5. Cogroup για ομαδοποίηση σημείων ανά κελί
            JavaPairRDD<String, Tuple2<Iterable<Point>, Iterable<Point>>> grouped =
                    rCells.cogroup(sCells);

            // 6. Υπολογισμός ζευγών και καταμέτρηση
            JavaPairRDD<String, Integer> counts = grouped.flatMapToPair(pair -> {
                        List<Tuple2<String, String>> candidatePairs = new ArrayList<>();
                        Iterable<Point> rPointsInCell = pair._2()._1();
                        Iterable<Point> sPointsInCell = pair._2()._2();

                        for (Point r : rPointsInCell) {
                            for (Point s : sPointsInCell) {
                                if (r.distance(s) <= epsilon) {
                                    candidatePairs.add(new Tuple2<>(r.id, s.id));
                                }
                            }
                        }
                        return candidatePairs.iterator();
                    })
                    .distinct()
                    .mapToPair(pair -> new Tuple2<>(pair._1, 1))
                    .reduceByKey(Integer::sum)
                    .filter(pair -> pair._2() > k);

            // 7. Αποθήκευση αποτελεσμάτων
            JavaRDD<String> output = counts.map(pair -> "{" + pair._1 + ", " + pair._2 + "}");
            output.saveAsTextFile(outputPath);

            List<String> lines = output.collect();

            try (PrintWriter out = new PrintWriter("resultsB.txt")) {
                for (String line : lines) {
                    out.println(line);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

            // 8. Εκτύπωση στατιστικών

            long start = System.currentTimeMillis();
            long count = output.count();
            long end = System.currentTimeMillis();

            double seconds = (end - start) / 1000.0;
            System.out.println("Execution time: " + seconds + " sec");

            System.out.println("R points with > " + k + " neighbors: " + counts.count());
        }
    }

    // Grid partitioning με 3x3 γειτονικά κελιά
    private static JavaPairRDD<String, Point> createGridRDD(JavaRDD<Point> points, double gridSize) {
        return points.flatMapToPair(point -> {
            List<Tuple2<String, Point>> cells = new ArrayList<>();
            int xCell = (int) Math.floor(point.x / gridSize);
            int yCell = (int) Math.floor(point.y / gridSize);
            for (int dx = -1; dx <= 1; dx++) {
                for (int dy = -1; dy <= 1; dy++) {
                    String cellKey = (xCell + dx) + "_" + (yCell + dy);
                    cells.add(new Tuple2<>(cellKey, point));
                }
            }
            return cells.iterator();
        });
    }

    public static class Point implements java.io.Serializable {
        public final String id;
        public final double x;
        public final double y;

        public Point(String id, double x, double y) {
            this.id = id;
            this.x = x;
            this.y = y;
        }

        public static Point fromCSV(String line) {
            String[] tokens = line.trim().split("\t");
            if (tokens.length < 3) return null;
            try {
                return new Point(
                        tokens[0].trim(),
                        Double.parseDouble(tokens[1].trim()),
                        Double.parseDouble(tokens[2].trim())
                );
            } catch (NumberFormatException e) {
                return null;
            }
        }

        public double distance(Point other) {
            double dx = this.x - other.x;
            double dy = this.y - other.y;
            return Math.sqrt(dx*dx + dy*dy);
        }
    }
}

