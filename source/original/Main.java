/*package org.example;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.HashPartitioner;
import scala.Tuple2;
import java.util.*;

public class Main {
    public static void main(String[] args) {
        // 1. Διαβάζουμε την παράμετρο ε από τη γραμμή εντολών
        double epsilon = args.length > 0 ? Double.parseDouble(args[0]) : 0.5;

        // 2. Ρυθμίσεις Spark για βελτιστοποιημένη απόδοση
        SparkConf conf = new SparkConf()
                .setAppName("SpatialQueryA")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .registerKryoClasses(new Class[]{Point.class});

        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3. Φόρτωση δεδομένων από HDFS (προσαρμόστε το path αν χρειάζεται)
        JavaRDD<String> rData = sc.textFile("hdfs:///user/hduser/input/RAILS.csv");
        JavaRDD<String> sData = sc.textFile("hdfs:///user/hduser/input/AREALM.csv");

        // 4. Μετατροπή σε αντικείμενα Point με έλεγχο για σφάλματα
        JavaRDD<Point> rPoints = rData.map(Point::fromCSV).filter(Objects::nonNull);
        JavaRDD<Point> sPoints = sData.map(Point::fromCSV).filter(Objects::nonNull);

        // 5. Βέλτιστο grid size για ελαχιστοποίηση partitions
        double gridSize = epsilon / Math.sqrt(2);

        // 6. Δημιουργία grid cells και replication
        JavaPairRDD<String, Point> rCells = createGridRDD(rPoints, gridSize)
                .partitionBy(new HashPartitioner(200));

        JavaPairRDD<String, Point> sCells = createGridRDD(sPoints, gridSize)
                .partitionBy(new HashPartitioner(200));

        // 7. Join και υπολογισμός αποστάσεων
        JavaRDD<String> results = rCells
                .join(sCells)
                .map(pair -> pair._2())
                .filter(pair -> pair._1.distance(pair._2) <= epsilon)
                .map(pair -> pair._1.id + "," + pair._2.id)
                .distinct();

        // 8. Αποθήκευση αποτελεσμάτων στο HDFS
        results.saveAsTextFile("hdfs:///user/hduser/output/resultsA_" + epsilon);

        // 9. Εκτύπωση πλήθους (προαιρετικό)
        long count = results.count();
        System.out.println("Total pairs within ε=" + epsilon + ": " + count);

        sc.stop();
    }

    // Δημιουργεί RDD με grid cells και γειτονικές κυψέλες

    private static JavaPairRDD<String, Point> createGridRDD(JavaRDD<Point> points, double gridSize) {
        return points.flatMapToPair(point -> {
            List<Tuple2<String, Point>> cells = new ArrayList<>();
            int xCell = (int) Math.floor(point.x / gridSize);
            int yCell = (int) Math.floor(point.y / gridSize);

            // Προσθήκη 3x3 γειτονικών κυψελών
            for (int dx = -1; dx <= 1; dx++) {
                for (int dy = -1; dy <= 1; dy++) {
                    String cellKey = (xCell + dx) + "_" + (yCell + dy);
                    cells.add(new Tuple2<>(cellKey, point));
                }
            }
            return cells.iterator();
        });
    }

    // Κλάση για αναπαράσταση σημείων με ελέγχους σφαλμάτων

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
            // Προσαρμόστε το split αν τα δεδομένα έχουν διαφορετικό διαχωριστικό
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


/*package org.example;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.HashPartitioner;
import scala.Tuple2;
import java.util.*;

public class Main {
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
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import java.util.*;

public class Main {
    public static void main(String[] args) {
        // 1. Επαλήθευση παραμέτρων
        if (args.length < 2) {
            System.err.println("Usage: QueryB <epsilon> <k>");
            System.exit(1);
        }

        // 2. Ανάγνωση παραμέτρων
        double epsilon = Double.parseDouble(args[0]);
        int k = Integer.parseInt(args[1]);

        // 3. Ρυθμίσεις Spark για βέλτιστη απόδοση
        SparkConf conf = new SparkConf()
                .setAppName("SpatialQueryB")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("spark.kryo.registrationRequired", "true")
                .registerKryoClasses(new Class[]{Point.class, Tuple2.class});

        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            // 4. Φόρτωση και προεπεξεργασία δεδομένων
            JavaRDD<Point> rPoints = loadPoints(sc, "RAILS.csv");
            JavaRDD<Point> sPoints = loadPoints(sc, "AREALM.csv");

            // 5. Υπολογισμός grid size με έλεγχο για μηδενική τιμή
            if (epsilon <= 0) {
                throw new IllegalArgumentException("Epsilon must be positive");
            }
            double gridSize = epsilon / Math.sqrt(2);

            // 6. Βέλτιστος αριθμός partitions
            int numPartitions = Math.max(sc.defaultParallelism() * 4, 1000);

            // 7. Δημιουργία grid cells με caching
            JavaPairRDD<String, Point> rCells = createGridRDD(rPoints, gridSize)
                    .partitionBy(new HashPartitioner(numPartitions))
                    .persist(StorageLevel.MEMORY_AND_DISK_SER());

            JavaPairRDD<String, Iterable<Point>> sGroups = createGridRDD(sPoints, gridSize)
                    .groupByKey()
                    .partitionBy(new HashPartitioner(numPartitions))
                    .persist(StorageLevel.MEMORY_AND_DISK_SER());

            // 8. Χωρική ένωση και καταμέτρηση
            JavaPairRDD<String, Integer> counts = rCells
                    .join(sGroups)  // (cellKey, (Point, Iterable<Point>))
                    .mapToPair(pair -> {
                        Point rPoint = pair._2()._1();
                        Iterable<Point> sPointsInCell = pair._2()._2();

                        int count = 0;
                        for (Point sPoint : sPointsInCell) {
                            if (rPoint.distance(sPoint) <= epsilon) {
                                count++;
                            }
                        }
                        return new Tuple2<>(rPoint.id, count);
                    })
                    .reduceByKey(Integer::sum)
                    .filter(pair -> pair._2() > k);

            // 9. Αποθήκευση αποτελεσμάτων
            JavaRDD<String> output = counts.map(pair -> "{" + pair._1 + ", " + pair._2 + "}");
            output.saveAsTextFile("hdfs:///user/hduser/output/resultsB_" + epsilon + "_" + k);

            // 10. Εκτύπωση στατιστικών
            long resultCount = counts.count();
            System.out.println("R points with > " + k + " neighbors: " + resultCount);

            // 11. Εκκαθάριση μνήμης
            rCells.unpersist();
            sGroups.unpersist();
        } catch (Exception e) {
            System.err.println("Error in Spark job: " + e.getMessage());
            e.printStackTrace();
            System.exit(2);
        }
    }

    //  Φορτώνει και επεξεργάζεται δεδομένα από HDFS

    private static JavaRDD<Point> loadPoints(JavaSparkContext sc, String filename) {
        return sc.textFile("hdfs:///user/hduser/input/" + filename)
                .map(Point::fromCSV)
                .filter(Objects::nonNull)
                .persist(StorageLevel.MEMORY_AND_DISK_SER());
    }

    //  Δημιουργεί RDD με grid cells και γειτονικές κυψέλες

    private static JavaPairRDD<String, Point> createGridRDD(JavaRDD<Point> points, double gridSize) {
        return points.flatMapToPair(point -> {
            List<Tuple2<String, Point>> cells = new ArrayList<>();
            int xCell = (int) Math.floor(point.x / gridSize);
            int yCell = (int) Math.floor(point.y / gridSize);

            // Προσθήκη 3x3 γειτονικών κυψελών
            for (int dx = -1; dx <= 1; dx++) {
                for (int dy = -1; dy <= 1; dy++) {
                    String cellKey = (xCell + dx) + "_" + (yCell + dy);
                    cells.add(new Tuple2<>(cellKey, point));
                }
            }
            return cells.iterator();
        });
    }

    //  Κλάση για αναπαράσταση γεωχωρικών σημείων

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

            // Έλεγχος για έγκυρη γραμμή
            if (tokens.length < 3) {
                System.err.println("Invalid line (missing columns): " + line);
                return null;
            }

            // Έλεγχος για κενά πεδία
            if (tokens[0].isEmpty() || tokens[1].isEmpty() || tokens[2].isEmpty()) {
                System.err.println("Invalid line (empty fields): " + line);
                return null;
            }

            try {
                double x = Double.parseDouble(tokens[1].trim());
                double y = Double.parseDouble(tokens[2].trim());

                // Προαιρετικός έλεγχος για έγκυρες συντεταγμένες
                if (Math.abs(x) > 180 || Math.abs(y) > 90) {
                    System.err.println("Suspicious coordinates: " + line);
                }

                return new Point(tokens[0].trim(), x, y);
            } catch (NumberFormatException e) {
                System.err.println("Number format error in line: " + line);
                System.err.println("Error details: " + e.getMessage());
                return null;
            }
        }

        public double distance(Point other) {
            // Έλεγχος για μηδενικό σημείο
            if (other == null) return Double.POSITIVE_INFINITY;

            double dx = this.x - other.x;
            double dy = this.y - other.y;
            return Math.sqrt(dx*dx + dy*dy);
        }

        @Override
        public String toString() {
            return String.format("Point(%s, %.6f, %.6f)", id, x, y);
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
import java.io.PrintWriter;

public class Main {
    public static void main(String[] args) {
        // Έλεγχος arguments
        if (args.length < 4) {
            System.err.println("Usage: Main <R_path> <S_path> <Output_path> <epsilon>");
            System.exit(1);
        }

        String rPath = args[0];
        String sPath = args[1];
        String outputPath = args[2];
        double epsilon = Double.parseDouble(args[3]);

        SparkConf conf = new SparkConf()
                .setAppName("SpatialQueryA")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .registerKryoClasses(new Class[]{Point.class});

        JavaSparkContext sc = new JavaSparkContext(conf);

        // 1. Φόρτωση δεδομένων
        JavaRDD<String> rData = sc.textFile(rPath);
        JavaRDD<String> sData = sc.textFile(sPath);

        // 2. Μετατροπή σε αντικείμενα Point
        JavaRDD<Point> rPoints = rData.map(Point::fromCSV).filter(Objects::nonNull);
        JavaRDD<Point> sPoints = sData.map(Point::fromCSV).filter(Objects::nonNull);

        // 3. Grid size
        double gridSize = epsilon / Math.sqrt(2);

        // 4. Δημιουργία grid cells
        JavaPairRDD<String, Point> rCells = createGridRDD(rPoints, gridSize)
                .partitionBy(new HashPartitioner(200));
        JavaPairRDD<String, Point> sCells = createGridRDD(sPoints, gridSize)
                .partitionBy(new HashPartitioner(200));

        // 5. Join και υπολογισμός αποστάσεων
        JavaRDD<String> results = rCells
                .join(sCells)
                .map(pair -> pair._2())
                .filter(pair -> pair._1.distance(pair._2) <= epsilon)
                .map(pair -> pair._1.id + "," + pair._2.id)
                .distinct();

        // 6. Αποθήκευση αποτελεσμάτων στο outputPath
        results.saveAsTextFile(outputPath);

        // 7. Εκτύπωση και αποθήκευση αριθμού ζευγών
        long start = System.currentTimeMillis();
        long count = results.count();
        long end = System.currentTimeMillis();

        double seconds = (end - start) / 1000.0;
        System.out.println("Total pairs within ε=" + epsilon + ": " + count);
        System.out.println("Execution time: " + seconds + " sec");

        // Αποθήκευση αριθμού ζευγών σε αρχείο στο output directory (στο τοπικό fs)
        // Αν θέλεις το count.txt στο HDFS, χρησιμοποίησε sc.parallelize
        //sc.parallelize(Collections.singletonList("Total pairs: " + count))
               // .saveAsTextFile(outputPath + "_count");
        sc.parallelize(Collections.singletonList("Total pairs: " + count + " (execution time: " + seconds + " sec)"))
                .saveAsTextFile(outputPath + "_count");

        sc.stop();
    }

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

package org.example;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.HashPartitioner;
import scala.Tuple2;
import java.util.*;
import java.io.PrintWriter;

public class Main {
    public static void main(String[] args) {
        // Έλεγχος arguments
        if (args.length < 4) {
            System.err.println("Usage: Main <R_path> <S_path> <Output_path> <epsilon>");
            System.exit(1);
        }

        String rPath = args[0];
        String sPath = args[1];
        String outputPath = args[2];
        double epsilon = Double.parseDouble(args[3]);

        SparkConf conf = new SparkConf()
                .setAppName("SpatialQueryA")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .registerKryoClasses(new Class[]{Point.class});

        JavaSparkContext sc = new JavaSparkContext(conf);

        // 1. Φόρτωση δεδομένων
        JavaRDD<String> rData = sc.textFile(rPath);
        JavaRDD<String> sData = sc.textFile(sPath);

        // 2. Μετατροπή σε αντικείμενα Point
        JavaRDD<Point> rPoints = rData.map(Point::fromCSV).filter(Objects::nonNull);
        JavaRDD<Point> sPoints = sData.map(Point::fromCSV).filter(Objects::nonNull);

        // 3. Grid size
        double gridSize = epsilon / Math.sqrt(2);

        // 4. Δημιουργία grid cells
        JavaPairRDD<String, Point> rCells = createGridRDD(rPoints, gridSize)
                .partitionBy(new HashPartitioner(200));
        JavaPairRDD<String, Point> sCells = createGridRDD(sPoints, gridSize)
                .partitionBy(new HashPartitioner(200));

        // 5. Join και υπολογισμός αποστάσεων
        JavaRDD<String> results = rCells
                .join(sCells)
                .map(pair -> pair._2())
                .filter(pair -> pair._1.distance(pair._2) <= epsilon)
                .map(pair -> pair._1.id + "," + pair._2.id)
                .distinct();

        // 6. Αποθήκευση αποτελεσμάτων στο outputPath
        results.saveAsTextFile(outputPath);

        // 7. Εκτύπωση και αποθήκευση αριθμού ζευγών
        long start = System.currentTimeMillis();
        long count = results.count();
        long end = System.currentTimeMillis();

        double seconds = (end - start) / 1000.0;
        System.out.println("Total pairs within ε=" + epsilon + ": " + count);
        System.out.println("Execution time: " + seconds + " sec");

        // Αποθήκευση αριθμού ζευγών σε αρχείο στο output directory (στο τοπικό fs)
        // Αν θέλεις το count.txt στο HDFS, χρησιμοποίησε sc.parallelize
        //sc.parallelize(Collections.singletonList("Total pairs: " + count))
        // .saveAsTextFile(outputPath + "_count");
        sc.parallelize(Collections.singletonList("Total pairs: " + count + " (execution time: " + seconds + " sec)"))
                .saveAsTextFile(outputPath + "_count");

        try (PrintWriter out = new PrintWriter("resultsA.txt")) {
            out.println(count);
        } catch (Exception e) {
            e.printStackTrace();
        }

        sc.stop();
    }

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