package org.example;

import scala.Tuple2;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;

public class GeoQueryUtils {

    // Υπολογισμός ευκλείδειας απόστασης
    public static double euclideanDistance(MyPoint a, MyPoint b) {
        double dx = a.getX() - b.getX();
        double dy = a.getY() - b.getY();
        return Math.sqrt(dx * dx + dy * dy);
    }

    /**
     * Query A: Επιστρέφει το πλήθος όλων των ζευγαριών (r, s) με dist(r, s) ≤ e
     */
    public static long solveQueryA(JavaRDD<MyPoint> rPoints,
                                   JavaRDD<MyPoint> sPoints,
                                   double e) {

        // Κάρτες για όλα τα ζεύγη (r, s)
        return rPoints
                .cartesian(sPoints)
                .filter(tuple -> euclideanDistance(tuple._1, tuple._2) <= e)
                .count();
    }

    /**
     * Query B: Για κάθε r ∈ R, υπολογίζει πόσα s ∈ S είναι εντός απόστασης e,
     * έπειτα φιλτράρει εκείνα τα r που έχουν > k τέτοια s.
     * Επιστρέφει (rID, count) για όσα r πληρούν την συνθήκη.
     */
    public static JavaPairRDD<String, Long> solveQueryB(JavaRDD<MyPoint> rPoints,
                                                        JavaRDD<MyPoint> sPoints,
                                                        double e,
                                                        long k) {

        // Κάρτες + φιλτράρισμα βάσει απόστασης
        // mapToPair για καταμέτρηση ανά ID του r
        JavaPairRDD<String, Long> countsByR = rPoints
                .cartesian(sPoints)
                .filter(tuple -> euclideanDistance(tuple._1, tuple._2) <= e)
                .mapToPair(tuple -> new Tuple2<>(tuple._1.getId(), 1L))
                .reduceByKey(Long::sum);

        // Επιστρέφουμε μόνο όσους έχουν περισσότερα από k σημεία S σε απόσταση e
        return countsByR.filter(t -> t._2 > k);
    }
}

