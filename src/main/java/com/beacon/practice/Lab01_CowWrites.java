package com.beacon.practice;



import com.beacon.config.SparkConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Lab01_CowWrites {

    // ── Hudi table configuration ────────────────────────────────────────────
    static final String BASE_PATH  = "file:///C:/hudi/labs/beacon_transactions";
    static final String TABLE_NAME = "beacon_transactions";

    static Map<String, String> hudiOptions() {
        Map<String, String> opts = new HashMap<>();
        opts.put("hoodie.table.name",                           TABLE_NAME);
        opts.put("hoodie.datasource.write.table.type",          "COPY_ON_WRITE");
        opts.put("hoodie.datasource.write.operation",           "upsert");
        opts.put("hoodie.datasource.write.recordkey.field",     "parcelId");
        opts.put("hoodie.datasource.write.partitionpath.field", "area");
        opts.put("hoodie.datasource.write.precombine.field",    "transactionDate");
        opts.put("hoodie.upsert.shuffle.parallelism",           "2");
        opts.put("hoodie.insert.shuffle.parallelism",           "2");
        // Cleaning: keep last 5 commits for time travel
        opts.put("hoodie.cleaner.commits.retained",             "5");
        opts.put("hoodie.clean.automatic",                      "true");
        return opts;
    }

    // ── Schema matching PropertyTransaction ────────────────────────────────
    static StructType schema() {
        return new StructType()
                .add("parcelId",          DataTypes.StringType)
                .add("transactionId",     DataTypes.StringType)
                .add("transactionType",   DataTypes.StringType)
                .add("propertyType",      DataTypes.StringType)
                .add("area",              DataTypes.StringType)
                .add("city",              DataTypes.StringType)
                .add("buyerName",         DataTypes.StringType)
                .add("sellerName",        DataTypes.StringType)
                .add("transactionAmount", DataTypes.DoubleType)
                .add("currency",          DataTypes.StringType)
                .add("status",            DataTypes.StringType)
                .add("validationStatus",  DataTypes.StringType)
                .add("transactionDate",   DataTypes.StringType);
    }

    // ── Exercise 01-A: First write ─────────────────────────────────────────
    public static void exerciseA(SparkSession spark) {
        System.out.println("\n━━━ Exercise 01-A: First write ━━━");

        List<Row> batch1 = Arrays.asList(
                RowFactory.create("PCL-MM-001","TXN-001","PURCHASE",   "CONDO",       "Makati",      "Metro Manila","Juan dela Cruz",    "Prestige Props",    4_500_000.0,"PHP","PENDING","PENDING","2024-01-15T09:00:00"),
                RowFactory.create("PCL-MM-002","TXN-002","MORTGAGE",   "RESIDENTIAL", "Makati",      "Metro Manila","Maria Santos",      "Metro Land Corp",   3_200_000.0,"PHP","PENDING","PENDING","2024-01-15T09:05:00"),
                RowFactory.create("PCL-QC-001","TXN-003","TRANSFER",   "LAND",        "Quezon City", "Metro Manila","Jose Reyes",        "Golden Gate Realty",2_800_000.0,"PHP","PENDING","PENDING","2024-01-15T09:10:00"),
                RowFactory.create("PCL-QC-002","TXN-004","PURCHASE",   "COMMERCIAL",  "Quezon City", "Metro Manila","Ana Gonzalez",      "Pacific Homes",     8_100_000.0,"PHP","PENDING","PENDING","2024-01-15T09:15:00"),
                RowFactory.create("PCL-BGC-001","TXN-005","TITLE_REGISTRATION","CONDO","BGC",        "Metro Manila","Carlos Mendoza",    "Horizon Dev",       6_500_000.0,"PHP","PENDING","PENDING","2024-01-15T09:20:00")
        );

        Dataset<Row> df = spark.createDataFrame(batch1, schema());

        System.out.println("Writing batch 1 — 5 new records to Hudi CoW table...");
        df.write()
                .format("hudi")
                .options(hudiOptions())
                .mode("append")
                .save(BASE_PATH);

        System.out.println("✅ Batch 1 written.\n");

        // Verify: read back and show
        System.out.println("Reading back from Hudi:");
        spark.read().format("hudi").load(BASE_PATH)
                .select("parcelId", "transactionType", "area",
                        "transactionAmount", "validationStatus",
                        "_hoodie_commit_time")
                .orderBy("area", "parcelId")
                .show(10, false);
    }

    // ── Exercise 01-B: Second write (mix of new + updates) ─────────────────
    public static void exerciseB(SparkSession spark) {
        System.out.println("\n━━━ Exercise 01-B: Second write — updates + new records ━━━");
        System.out.println("Sending 3 records:");
        System.out.println("  PCL-MM-001 → validationStatus changes PENDING → VALID");
        System.out.println("  PCL-QC-001 → validationStatus changes PENDING → INVALID");
        System.out.println("  PCL-TG-001 → brand new parcel, never seen before\n");

        List<Row> batch2 = Arrays.asList(
                // UPDATE: PCL-MM-001 — validation team approved it
                RowFactory.create("PCL-MM-001","TXN-001","PURCHASE",   "CONDO",       "Makati",      "Metro Manila","Juan dela Cruz",    "Prestige Props",    4_500_000.0,"PHP","APPROVED","VALID",  "2024-01-15T10:00:00"),
                // UPDATE: PCL-QC-001 — validation team rejected it
                RowFactory.create("PCL-QC-001","TXN-003","TRANSFER",   "LAND",        "Quezon City", "Metro Manila","Jose Reyes",        "Golden Gate Realty",2_800_000.0,"PHP","REJECTED","INVALID","2024-01-15T10:05:00"),
                // INSERT: new parcel from Taguig
                RowFactory.create("PCL-TG-001","TXN-006","PURCHASE",   "RESIDENTIAL", "Taguig",      "Metro Manila","Luz Bautista",      "Meridian Realty",   5_200_000.0,"PHP","PENDING", "PENDING","2024-01-15T10:10:00")
        );

        Dataset<Row> df = spark.createDataFrame(batch2, schema());

        System.out.println("Writing batch 2...");
        df.write()
                .format("hudi")
                .options(hudiOptions())
                .mode("append")
                .save(BASE_PATH);

        System.out.println("✅ Batch 2 written.\n");

        // Read back — note the validationStatus has changed for MM-001 and QC-001
        System.out.println("Table after batch 2 — notice updated validationStatus:");
        spark.read().format("hudi").load(BASE_PATH)
                .select("parcelId", "transactionType", "area",
                        "status", "validationStatus", "_hoodie_commit_time")
                .orderBy("area", "parcelId")
                .show(10, false);

        System.out.println("KEY OBSERVATION:");
        System.out.println("  PCL-MM-001: validationStatus = VALID   (was PENDING)");
        System.out.println("  PCL-QC-001: validationStatus = INVALID (was PENDING)");
        System.out.println("  PCL-TG-001: new record added");
        System.out.println("  Total records = 6, not 8 (no duplicates — UPSERT worked)\n");
    }

    // ── Exercise 01-C: Inspect the timeline ────────────────────────────────
    public static void exerciseC(SparkSession spark) {
        System.out.println("\n━━━ Exercise 01-C: Inspect the Hudi timeline ━━━");
        System.out.println("Open Windows Explorer and navigate to:");
        System.out.println("  C:\\hudi\\labs\\beacon_transactions\\.hoodie\\\n");
        System.out.println("You should see files like:");
        System.out.println("  hoodie.properties");
        System.out.println("  <timestamp1>.commit.requested");
        System.out.println("  <timestamp1>.commit");
        System.out.println("  <timestamp2>.commit.requested");
        System.out.println("  <timestamp2>.commit");
        System.out.println("  metadata/   (Hudi's internal index)\n");

        // Show it programmatically too
        System.out.println("Reading timeline via Spark:");
        spark.read().format("hudi").load(BASE_PATH)
                .select("_hoodie_commit_time", "_hoodie_record_key",
                        "_hoodie_partition_path", "_hoodie_file_name")
                .orderBy("_hoodie_commit_time", "_hoodie_record_key")
                .show(10, false);

        System.out.println("WHAT THE METADATA COLUMNS TELL YOU:");
        System.out.println("  _hoodie_commit_time   → which commit wrote this version of the record");
        System.out.println("  _hoodie_record_key    → the parcelId (your record key)");
        System.out.println("  _hoodie_partition_path → area=Makati / area=BGC etc.");
        System.out.println("  _hoodie_file_name     → which Parquet file this record lives in\n");

        System.out.println("NOTICE: PCL-MM-001 and PCL-QC-001 have a LATER _hoodie_commit_time");
        System.out.println("        than PCL-MM-002. That is proof the upsert wrote a new version.\n");
    }

    public static void writeBatch3(SparkSession spark) {
        System.out.println("\n[Lab 02 setup] Writing batch 3 — 2 more records...");

        List<Row> batch3 = Arrays.asList(
                // UPDATE: PCL-BGC-001 — now validated
                RowFactory.create("PCL-BGC-001","TXN-005","TITLE_REGISTRATION","CONDO","BGC",
                        "Metro Manila","Carlos Mendoza","Horizon Dev",6_500_000.0,"PHP",
                        "APPROVED","VALID","2024-01-15T11:00:00"),
                // INSERT: new Pasig parcel
                RowFactory.create("PCL-PS-001","TXN-007","LEASE","COMMERCIAL","Pasig",
                        "Metro Manila","Ramon Torres","Pacific Homes",1_800_000.0,"PHP",
                        "PENDING","PENDING","2024-01-15T11:05:00")
        );

        spark.createDataFrame(batch3, schema())
                .write().format("hudi").options(hudiOptions())
                .mode("append").save(BASE_PATH);

        System.out.println("✅ Batch 3 written. Table now has 3 commits.\n");
    }

    // ── Main ───────────────────────────────────────────────────────────────
    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkConfig.createSession("Lab01-CowWrites");

        exerciseA(spark);  // First write — all inserts
        exerciseB(spark);  // Second write — updates + new insert
        writeBatch3(spark); // Write batch 3 for Lab 02 setup
        exerciseC(spark);  // Inspect timeline metadata

        spark.stop();
    }
}