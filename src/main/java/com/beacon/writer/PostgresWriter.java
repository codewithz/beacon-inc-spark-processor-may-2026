package com.beacon.writer;

import com.beacon.config.PostgresConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.List;

/**
 * Writes a micro-batch DataFrame to PostgreSQL using UPSERT.
 *
 * Uses foreachBatch — Spark calls this once per micro-batch with
 * a complete DataFrame. We iterate the rows and upsert each one
 * using INSERT ... ON CONFLICT (parcel_id) DO UPDATE.
 *
 * Why foreachBatch instead of Spark's built-in JDBC sink?
 * The built-in JDBC sink only does append — no upsert support.
 * foreachBatch gives us full control over the write logic.
 */
public class PostgresWriter {
    private static final Logger log = LoggerFactory.getLogger(PostgresWriter.class);

    private static final String UPSERT_SQL = """
            INSERT INTO property_transactions (
                parcel_id, transaction_id, transaction_type, property_type,
                area, city, buyer_name, seller_name,
                transaction_amount, currency, title_deed_uri,
                status, validation_status, transaction_date,
                kafka_partition, kafka_offset, last_updated
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW())
            ON CONFLICT (parcel_id) DO UPDATE SET
                transaction_id      = EXCLUDED.transaction_id,
                transaction_type    = EXCLUDED.transaction_type,
                property_type       = EXCLUDED.property_type,
                area                = EXCLUDED.area,
                city                = EXCLUDED.city,
                buyer_name          = EXCLUDED.buyer_name,
                seller_name         = EXCLUDED.seller_name,
                transaction_amount  = EXCLUDED.transaction_amount,
                currency            = EXCLUDED.currency,
                title_deed_uri      = EXCLUDED.title_deed_uri,
                status              = EXCLUDED.status,
                validation_status   = EXCLUDED.validation_status,
                transaction_date    = EXCLUDED.transaction_date,
                kafka_partition     = EXCLUDED.kafka_partition,
                kafka_offset        = EXCLUDED.kafka_offset,
                last_updated        = NOW()
            """;

    /**
     * Called by Spark for every micro-batch.
     * batchId is the Spark-assigned batch sequence number — useful for logging.
     */
    public void write(Dataset<Row> batchDF, long batchId) {

        long count = batchDF.count();

        if (count == 0) {
            log.debug("Batch {} — empty, skipping", batchId);
            return;
        }

        log.info("Batch {} — writing {} records to PostgreSQL", batchId, count);

        // Collect rows to the driver — acceptable for micro-batch volumes
        // For very high volume, use JDBC batch writes directly from executors
        List<Row> rows = batchDF.collectAsList();

        try (Connection conn = DriverManager.getConnection(
                PostgresConfig.URL, PostgresConfig.USER, PostgresConfig.PASSWORD);
             PreparedStatement stmt = conn.prepareStatement(UPSERT_SQL)) {

            conn.setAutoCommit(false); // batch everything in one transaction

            int inserted = 0;
            int failed   = 0;

            for (Row row : rows) {
                try {
                    stmt.setString(1,  safeString(row, "parcelId"));
                    stmt.setString(2,  safeString(row, "transactionId"));
                    stmt.setString(3,  safeString(row, "transactionType"));
                    stmt.setString(4,  safeString(row, "propertyType"));
                    stmt.setString(5,  safeString(row, "area"));
                    stmt.setString(6,  safeString(row, "city"));
                    stmt.setString(7,  safeString(row, "buyerName"));
                    stmt.setString(8,  safeString(row, "sellerName"));
                    stmt.setDouble(9,  safeDouble(row, "transactionAmount"));
                    stmt.setString(10, safeString(row, "currency"));
                    stmt.setString(11, safeString(row, "titleDeedUri"));
                    stmt.setString(12, safeString(row, "status"));
                    stmt.setString(13, safeString(row, "validationStatus"));
                    stmt.setTimestamp(14, safeTimestamp(row, "transactionDate")); // stored as string, Postgres casts it
                    stmt.setInt(15,    safeInt(row,    "kafka_partition"));
                    stmt.setLong(16,   safeLong(row,   "kafka_offset"));

                    stmt.addBatch();
                    inserted++;

                } catch (Exception e) {
                    log.warn("Batch {} — skipping row parcelId={} error={}",
                            batchId, safeString(row, "parcelId"), e.getMessage());
                    failed++;
                }
            }

            stmt.executeBatch();
            conn.commit();

            log.info("Batch {} — committed {} upserts, {} skipped", batchId, inserted, failed);

        } catch (SQLException e) {
            log.error("Batch {} — PostgreSQL write failed: {}", batchId, e.getMessage(), e);
            throw new RuntimeException("PostgreSQL write failed in batch " + batchId, e);
        }
    }

    // ── Safe field extractors ─────────────────────────────────────────────────

    private String safeString(Row row, String field) {
        try {
            int idx = row.fieldIndex(field);
            return row.isNullAt(idx) ? null : row.getString(idx);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    private double safeDouble(Row row, String field) {
        try {
            int idx = row.fieldIndex(field);
            return row.isNullAt(idx) ? 0.0 : row.getDouble(idx);
        } catch (IllegalArgumentException e) {
            return 0.0;
        }
    }

    private int safeInt(Row row, String field) {
        try {
            int idx = row.fieldIndex(field);
            return row.isNullAt(idx) ? 0 : row.getInt(idx);
        } catch (IllegalArgumentException e) {
            return 0;
        }
    }

    private long safeLong(Row row, String field) {
        try {
            int idx = row.fieldIndex(field);
            return row.isNullAt(idx) ? 0L : row.getLong(idx);
        } catch (IllegalArgumentException e) {
            return 0L;
        }
    }

    private java.sql.Timestamp safeTimestamp(Row row, String field) {
        try {
            int idx = row.fieldIndex(field);
            if (row.isNullAt(idx)) return null;

            String raw = row.getString(idx);
            if (raw == null || raw.isBlank()) return null;

            // Handles format: 2026-05-21T09:19:18
            return java.sql.Timestamp.valueOf(raw.replace("T", " "));

        } catch (Exception e) {
            log.warn("Could not parse transactionDate '{}' — inserting null",
                    safeString(row, field));
            return null;
        }
    }
}
