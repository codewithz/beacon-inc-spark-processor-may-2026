package com.beacon.processor;

import com.beacon.schema.TransactionSchema;
import com.beacon.writer.PostgresWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.spark.sql.functions.*;

public class TransactionProcessorPostgres {

    private static final Logger log = LoggerFactory.getLogger(TransactionProcessor.class);

    private final SparkSession spark;
    private final String kafkaBootstrapServers;
    private final String kafkaTopic;

    public TransactionProcessorPostgres(SparkSession spark,
                                String kafkaBootstrapServers,
                                String kafkaTopic) {
        this.spark = spark;
        this.kafkaBootstrapServers = kafkaBootstrapServers;
        this.kafkaTopic = kafkaTopic;
    }

    public StreamingQuery start() throws Exception {

        // ── Step 1: Read from Kafka ───────────────────────────────────────────
        Dataset<Row> rawStream = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", kafkaBootstrapServers)
                .option("subscribe", kafkaTopic)
                .option("startingOffsets", "earliest")
                .option("failOnDataLoss", "false")
                .load();

        // ── Step 2: Parse JSON ────────────────────────────────────────────────
        Dataset<Row> parsed = rawStream
                .select(
                        from_json(col("value").cast("string"), TransactionSchema.get())
                                .alias("txn"),
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

        // ── Step 3: Write to PostgreSQL via foreachBatch ──────────────────────
        PostgresWriter writer = new PostgresWriter();

        StreamingQuery query = parsed.writeStream()
                .foreachBatch(writer::write)
                .option("checkpointLocation", "C:/tmp/beacon-checkpoints/postgres")
                .trigger(Trigger.ProcessingTime("15 seconds"))
                .outputMode("update")
                .start();

        log.info("Streaming query started → PostgreSQL sink active");
        return query;
    }
}
