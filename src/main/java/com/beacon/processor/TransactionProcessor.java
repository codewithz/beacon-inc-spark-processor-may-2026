package com.beacon.processor;

import com.beacon.schema.TransactionSchema;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.spark.sql.functions.*;

/**
 * Reads from Kafka topic using Spark Structured Streaming.
 *
 * Pipeline for this iteration:
 *   1. Read raw bytes from Kafka
 *   2. Parse JSON value using TransactionSchema
 *   3. Select and cast typed columns
 *   4. Print to console — confirm data is flowing before we write to Hudi
 */
public class TransactionProcessor {

    private static final Logger log = LoggerFactory.getLogger(TransactionProcessor.class);

    private final SparkSession spark;
    private final String kafkaBootstrapServers;
    private final String kafkaTopic;

    public TransactionProcessor(SparkSession spark,
                                String kafkaBootstrapServers,
                                String kafkaTopic) {
        this.spark = spark;
        this.kafkaBootstrapServers = kafkaBootstrapServers;
        this.kafkaTopic = kafkaTopic;
    }

    public StreamingQuery start() throws Exception {
        // ── Step 1: Read raw stream from Kafka ───────────────────────────────
        Dataset<Row> rawStream = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", kafkaBootstrapServers)
                .option("subscribe", kafkaTopic)
                .option("startingOffsets", "earliest")  // read from beginning on first run
                .option("failOnDataLoss", "false")
                .load();

        log.info("Connected to Kafka | servers={} topic={}", kafkaBootstrapServers, kafkaTopic);

        // ── Step 2: Parse JSON from Kafka value column ───────────────────────
        // Kafka delivers: key (binary), value (binary), topic, partition, offset, timestamp
        // We cast value to String, then parse it as JSON using our schema

        Dataset<Row> parsed = rawStream
                .select(
                        col("key").cast("string").alias("kafka_key"),
                        col("partition").alias("kafka_partition"),
                        col("offset").alias("kafka_offset"),
                        col("timestamp").alias("kafka_timestamp"),
                        from_json(col("value").cast("string"), TransactionSchema.get())
                                .alias("txn")
                )
                .select(
                        col("kafka_key"),
                        col("kafka_partition"),
                        col("kafka_offset"),
                        col("kafka_timestamp"),
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
                        col("txn.transactionDate")
                );

        // ── Step 3: Write to console — confirm everything is wired ───────────
        // truncate(false) shows full column values, not cut off at 20 chars

        StreamingQuery query = parsed.writeStream()
                .format("console")
                .option("truncate", "false")
                .option("numRows", "50")
                .trigger(Trigger.ProcessingTime("10 seconds"))  // micro-batch every 10s
                .outputMode("append")
                .start();

        log.info("Streaming query started | awaiting data from topic: {}", kafkaTopic);

        return query;
    }

}