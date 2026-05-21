package com.beacon.config;

import org.apache.spark.sql.SparkSession;

/**
 * Builds and returns the SparkSession.
 * master("local[*4]") uses 4  CPU cores on the local machine.
 * When you move to a cluster, remove the master() line — it gets set by spark-submit.
 */
public class SparkConfig {

    public static SparkSession createSession() {
        return SparkSession.builder()
                .appName("BeaconSparkConsumer")
                .master("local[4]")                         // remove for cluster
                .config("spark.sql.shuffle.partitions", "3") // match Kafka topic partitions
                .config("spark.ui.port", "4040")
                .getOrCreate();
    }
}