package com.beacon.processor;



import com.beacon.config.HdfsConfig;
import com.beacon.schema.TransactionSchema;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static org.apache.spark.sql.functions.*;

public class TransactionProcessorHdfs {

    private static final Logger log = LoggerFactory.getLogger(TransactionProcessorHdfs.class);

    private final SparkSession spark;
    private final String kafkaBootstrapServers;
    private final String kafkaTopic;

    public TransactionProcessorHdfs(SparkSession spark,
                                String kafkaBootstrapServers,
                                String kafkaTopic) {
        this.spark = spark;
        this.kafkaBootstrapServers = kafkaBootstrapServers;
        this.kafkaTopic = kafkaTopic;
    }

    public StreamingQuery start() throws Exception {

        // ── Step 1: Read from Kafka ───────────────────────────────────────
        Dataset<Row> rawStream = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", kafkaBootstrapServers)
                .option("subscribe", kafkaTopic)
                .option("startingOffsets", "earliest")
                .option("failOnDataLoss", "false")
                .load();

        // ── Step 2: Parse JSON ────────────────────────────────────────────
        Dataset<Row> parsed = rawStream
                .select(
                        from_json(col("value").cast("string"),
                                TransactionSchema.get()).alias("txn"),
                        col("partition").alias("kafka_partition"),
                        col("offset").alias("kafka_offset")
                )
                .select(
                        col("txn.transactionId"),
                        col("txn.parcelId"),
                        col("txn.transactionType"),
                        col("txn.propertyType"),
                        col("txn.area"),
                        col("txn.city"),
                        col("txn.buyerName"),
                        col("txn.sellerName"),
                        col("txn.transactionAmount"),
                        col("txn.currency"),
                        col("txn.titleDeedUri"),
                        col("txn.status"),
                        col("txn.validationStatus"),
                        col("txn.transactionDate"),
                        col("kafka_partition"),
                        col("kafka_offset")
                )
                .filter(col("parcelId").isNotNull());

        // ── Step 3: Write to Hudi on HDFS ─────────────────────────────────
        StreamingQuery query = parsed.writeStream()
                .format("hudi")
                .options(hudiOptions())
                .option("checkpointLocation", "hdfs://localhost:8020/hudi/checkpoints/transactions")
                .trigger(Trigger.ProcessingTime("30 seconds"))
                .outputMode("append")
                .start("hdfs://localhost:8020/hudi/beacon_transactions");

        log.info("=== Spark → Hudi → HDFS pipeline started ===");
        log.info("Kafka topic  : {}", kafkaTopic);
        log.info("Hudi table   : {}", HdfsConfig.HUDI_BASE_PATH);
        log.info("Record key   : {}", HdfsConfig.RECORD_KEY_FIELD);
        log.info("Partition by : {}", HdfsConfig.PARTITION_FIELD);

        return query;
    }

    private Map<String, String> hudiOptions() {
        Map<String, String> opts = new HashMap<>();

        // ── Table identity ─────────────────────────────────────────────────
        opts.put("hoodie.table.name",                             HdfsConfig.TABLE_NAME);

        // ── Key configuration ──────────────────────────────────────────────
        opts.put("hoodie.datasource.write.recordkey.field",       HdfsConfig.RECORD_KEY_FIELD);
        opts.put("hoodie.datasource.write.partitionpath.field",   HdfsConfig.PARTITION_FIELD);
        opts.put("hoodie.datasource.write.precombine.field",      HdfsConfig.PRECOMBINE_FIELD);

        // ── Table type and operation ───────────────────────────────────────
        opts.put("hoodie.datasource.write.table.type",            "COPY_ON_WRITE");
        opts.put("hoodie.datasource.write.operation",             "upsert");

        // ── Index ──────────────────────────────────────────────────────────
        opts.put("hoodie.index.type",                             "BLOOM");

        // ── Cleaning ───────────────────────────────────────────────────────
        opts.put("hoodie.cleaner.policy",                         "KEEP_LATEST_COMMITS");
        opts.put("hoodie.cleaner.commits.retained",               "10");
        opts.put("hoodie.clean.automatic",                        "true");

        // ── File sizing ────────────────────────────────────────────────────
        opts.put("hoodie.parquet.max.file.size",
                String.valueOf(128 * 1024 * 1024));   // 128MB
        opts.put("hoodie.parquet.small.file.limit",
                String.valueOf(104 * 1024 * 1024));   // 104MB

        // ── Schema ────────────────────────────────────────────────────────
        opts.put("hoodie.datasource.write.reconcile.schema",      "true");

        // ── HDFS-specific: disable Hive sync for POC ───────────────────────
        opts.put("hoodie.datasource.hive_sync.enable",            "false");

        opts.put("hoodie.metadata.enable", "false");

        return opts;
    }
}