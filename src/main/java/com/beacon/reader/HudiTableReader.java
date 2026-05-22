package com.beacon.reader;

import com.beacon.config.HdfsConfig;
import com.beacon.config.SparkConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HudiTableReader {

    private static final Logger log = LoggerFactory.getLogger(HudiTableReader.class);

    public static void main(String[] args) {

        log.info("╔══════════════════════════════════════════════════╗");
        log.info("║     Beacon Inc — Hudi on HDFS: Proof of Work     ║");
        log.info("╚══════════════════════════════════════════════════╝");

        SparkSession spark = SparkConfig.createSession("HudiTableReader");

        // ── 1. Snapshot Read ──────────────────────────────────────
        log.info("\n>>> SNAPSHOT READ — all records currently in Hudi table");
        Dataset<Row> snapshot = spark.read()
                .format("hudi")
                .load(HdfsConfig.HUDI_BASE_PATH);

        snapshot.createOrReplaceTempView("beacon_transactions");

        long total = snapshot.count();
        log.info("Total records in Hudi table: {}", total);

        System.out.println("\n==================== ALL RECORDS (latest per parcelId) ====================");
        snapshot.select(
                "parcelId", "transactionId", "transactionType",
                "area", "transactionAmount", "currency",
                "status", "transactionDate",
                "_hoodie_commit_time"
        ).orderBy("area", "parcelId").show(50, false);


        // ── 2. Partition breakdown ────────────────────────────────
        System.out.println("\n==================== RECORDS PER AREA (PARTITION) ====================");
        spark.sql("""
                SELECT area, COUNT(*) as record_count,
                       ROUND(AVG(transactionAmount), 2) as avg_amount,
                       MAX(transactionAmount) as max_amount
                FROM beacon_transactions
                GROUP BY area
                ORDER BY record_count DESC
                """).show(false);


        // ── 3. Transaction type breakdown ─────────────────────────
        System.out.println("\n==================== RECORDS PER TRANSACTION TYPE ====================");
        spark.sql("""
                SELECT transactionType, COUNT(*) as count,
                       ROUND(SUM(transactionAmount), 2) as total_value
                FROM beacon_transactions
                GROUP BY transactionType
                ORDER BY count DESC
                """).show(false);


        // ── 4. Latest commits on the Hudi timeline ────────────────
        System.out.println("\n==================== HUDI COMMIT TIMELINE ====================");
        spark.sql("""
                SELECT _hoodie_commit_time, COUNT(*) as records_in_commit
                FROM beacon_transactions
                GROUP BY _hoodie_commit_time
                ORDER BY _hoodie_commit_time DESC
                """).show(false);


        // ── 5. Hudi metadata fields ───────────────────────────────
        System.out.println("\n==================== HUDI METADATA FIELDS (sample 5 rows) ====================");
        snapshot.select(
                "_hoodie_commit_time",
                "_hoodie_commit_seqno",
                "_hoodie_record_key",
                "_hoodie_partition_path",
                "_hoodie_file_name",
                "parcelId"
        ).show(5, false);


        // ── 6. Summary ────────────────────────────────────────────
        System.out.println("\n==================== PIPELINE SUMMARY ====================");
        System.out.println("  Source     : Apache Kafka → beacon.property.transactions");
        System.out.println("  Processing : Apache Spark Structured Streaming");
        System.out.println("  Storage    : Apache Hudi (CoW) on HDFS");
        System.out.println("  HDFS path  : " + HdfsConfig.HUDI_BASE_PATH);
        System.out.println("  Total rows : " + total);
        System.out.println("  Partitioned by: area");
        System.out.println("  Record key : parcelId (upsert mode)");
        System.out.println("==========================================================\n");

        spark.stop();
    }
}