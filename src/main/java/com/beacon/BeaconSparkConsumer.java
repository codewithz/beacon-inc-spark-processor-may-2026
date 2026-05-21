package com.beacon;

import com.beacon.config.SparkConfig;
import com.beacon.processor.TransactionProcessor;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BeaconSparkConsumer {

    private static final Logger log = LoggerFactory.getLogger(BeaconSparkConsumer.class);

    public static void main(String[] args) throws Exception {

        // Override via -D flags: mvn exec:java -Dkafka.servers=localhost:9092
        String kafkaServers = System.getProperty("kafka.servers", "localhost:9092");
        String kafkaTopic   = System.getProperty("kafka.topic",   "beacon.property.transactions");

        log.info("=== Beacon Spark Consumer Starting ===");
        log.info("Kafka servers : {}", kafkaServers);
        log.info("Kafka topic   : {}", kafkaTopic);

        SparkSession spark = SparkConfig.createSession();

        TransactionProcessor processor = new TransactionProcessor(
                spark,
                kafkaServers,
                kafkaTopic
        );

        StreamingQuery query = processor.start();

        // Block main thread — keep the streaming query alive
        query.awaitTermination();
    }
}
