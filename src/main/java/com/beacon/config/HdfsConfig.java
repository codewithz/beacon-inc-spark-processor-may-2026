package com.beacon.config;

/**
 * Central HDFS configuration.
 * All HDFS paths and connection settings live here.
 */
public class HdfsConfig {

    public static final String NAMENODE_URI      = "hdfs://localhost:8020";
    public static final String HUDI_BASE_PATH    = "hdfs://localhost:8020/hudi/beacon_transactions";
    public static final String CHECKPOINT_PATH   = "hdfs://localhost:8020/hudi/checkpoints/transactions";

    // ── Raw file storage paths ─────────────────────────────────────────────
    public static final String FILES_BASE_PATH  = NAMENODE_URI + "/files";
    public static final String PDF_PATH         = FILES_BASE_PATH + "/pdfs/titles";
    public static final String IMAGES_PATH      = FILES_BASE_PATH + "/images";

    // ── Hudi table configuration ───────────────────────────────────────────
    public static final String TABLE_NAME        = "beacon_transactions";
    public static final String RECORD_KEY_FIELD  = "parcelId";
    public static final String PARTITION_FIELD   = "area";
    public static final String PRECOMBINE_FIELD  = "transactionDate";
}
