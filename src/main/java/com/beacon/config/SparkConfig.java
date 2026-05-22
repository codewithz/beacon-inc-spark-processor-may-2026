package com.beacon.config;

import org.apache.spark.sql.SparkSession;

/**
 * Builds and returns the SparkSession.
 * master("local[*4]") uses 4  CPU cores on the local machine.
 * When you move to a cluster, remove the master() line — it gets set by spark-submit.
 */


public class SparkConfig {

    public static SparkSession createSession(String appName) {
        return SparkSession.builder()
                .appName(appName)
                .master("local[*]")

                // ── Required for Hudi ──────────────────────────────────────
                .config("spark.serializer",
                        "org.apache.spark.serializer.KryoSerializer")
                .config("spark.kryo.registrator",
                        "org.apache.spark.HoodieSparkKryoRegistrar")
                .config("spark.sql.extensions",
                        "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog",
                        "org.apache.spark.sql.hudi.catalog.HoodieCatalog")

                // ── Performance ────────────────────────────────────────────
                .config("spark.sql.shuffle.partitions", "3")

                // ── Suppress noisy logs ────────────────────────────────────
                .config("spark.ui.showConsoleProgress", "false")
                .getOrCreate();
    }
}