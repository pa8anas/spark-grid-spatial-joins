package org.example;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import scala.Tuple2;

public class Main {
    public static void main(String[] args) {
        // -------------------------------
        // 1. Ρύθμιση περιβάλλοντος Spark
        // -------------------------------
        SparkSession spark = SparkSession.builder()
                .appName("BDP Project - Επεξεργασία Γεωχωρικών Δεδομένων")
                .master("local[*]")  // Χρήση όλων των διαθέσιμων πυρήνων
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

        // -------------------------------
        // 2. Φόρτωση και μετατροπή δεδομένων R και S σε RDD<MyPoint>
        //    Υποθέτουμε ότι τα αρχεία είναι σε CSV/TSV με στήλες: ID, x, y
        // -------------------------------
        // Παράδειγμα ανάγνωσης αρχείων .tsv με header=true, delimiter='\t'
        Dataset<Row> dfR = spark.read()
                .option("header", "true")
                .option("delimiter", "\t")
                .csv("C:\\Users\\πανος\\Desktop\\project spark\\RAILS.tsv");

        Dataset<Row> dfS = spark.read()
                .option("header", "true")
                .option("delimiter", "\t")
                .csv("C:\\Users\\πανος\\Desktop\\project spark\\AREALM.tsv");

        // Μετατροπή σε RDD<MyPoint>
        JavaRDD<MyPoint> rPoints = dfR.javaRDD()
                .map(row -> {
                    String id = row.getString(0);        // π.χ. στήλη 0 = ID
                    double x = Double.parseDouble(row.getString(1)); // π.χ. στήλη 1 = x
                    double y = Double.parseDouble(row.getString(2)); // π.χ. στήλη 2 = y
                    return new MyPoint(id, x, y);
                });

        JavaRDD<MyPoint> sPoints = dfS.javaRDD()
                .map(row -> {
                    String id = row.getString(0);
                    double x = Double.parseDouble(row.getString(1));
                    double y = Double.parseDouble(row.getString(2));
                    return new MyPoint(id, x, y);
                });

        // -------------------------------
        // 3. Εκτέλεση του Query A
        // -------------------------------
        double e = 0.5; // Παράδειγμα threshold ε. Στην πράξη μπορείτε να το πάρετε ως είσοδο.
        long resultA = GeoQueryUtils.solveQueryA(rPoints, sPoints, e);

        // Εμφάνιση αποτελέσματος ή/και αποθήκευση σε αρχείο resultsA.txt
        System.out.println("Query A - Πλήθος ζευγαριών με dist(r, s) <= " + e + " : " + resultA);

        // -------------------------------
        // 4. Εκτέλεση του Query B
        // -------------------------------
        long k = 10; // Παράδειγμα για κ. Στην πράξη μπορείτε να το παίρνετε ως είσοδο.
        // Επιστρέφει JavaPairRDD<String, Long> με {rID, πλήθος s}
        // για όσα r έχουν count > k
        Dataset<Row> resultB = spark.createDataFrame(
                GeoQueryUtils.solveQueryB(rPoints, sPoints, e, k).collect(),
                Tuple2.class
        );

        // Παράδειγμα εμφάνισης των αποτελεσμάτων (rID, count)
        resultB.show();

        // Για να αποθηκεύσετε τα αποτελέσματα σε αρχείο resultsB.txt:
        // resultB.write().format("csv").save("path/to/resultsB.txt");
        // ή φτιάχνετε χειροκίνητα ένα αρχείο όπου γράφετε {rID, count} ανά γραμμή.

        // -------------------------------
        // 5. Τερματισμός Spark
        // -------------------------------
        spark.stop();
        jsc.close();
    }
}
