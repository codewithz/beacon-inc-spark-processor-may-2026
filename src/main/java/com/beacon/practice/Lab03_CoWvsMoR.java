package com.beacon.practice;



import com.beacon.config.SparkConfig;
import org.apache.hudi.common.util.Option;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;

import java.util.*;

import static org.apache.spark.sql.functions.*;

public class Lab03_CoWvsMoR {

    static final String COW_PATH = "file:///C:/hudi/labs/cow_table";
    static final String MOR_PATH = "file:///C:/hudi/labs/mor_table";

    static StructType schema() {
        return new StructType()
                .add("parcelId",          DataTypes.StringType)
                .add("transactionId",     DataTypes.StringType)
                .add("transactionType",   DataTypes.StringType)
                .add("area",              DataTypes.StringType)
                .add("transactionAmount", DataTypes.DoubleType)
                .add("validationStatus",  DataTypes.StringType)
                .add("transactionDate",   DataTypes.StringType);
    }

    static Map<String, String> baseOptions(String tableType, String tableName) {
        Map<String, String> opts = new HashMap<>();
        opts.put("hoodie.table.name",                           tableName);
        opts.put("hoodie.datasource.write.table.type",          tableType);
        opts.put("hoodie.datasource.write.operation",           "upsert");
        opts.put("hoodie.datasource.write.recordkey.field",     "parcelId");
        opts.put("hoodie.datasource.write.partitionpath.field", "area");
        opts.put("hoodie.datasource.write.precombine.field",    "transactionDate");
        opts.put("hoodie.upsert.shuffle.parallelism",           "2");
        opts.put("hoodie.insert.shuffle.parallelism",           "2");
        // MoR-specific: inline compaction OFF (we trigger it manually)
        opts.put("hoodie.compact.inline",                       "false");
        return opts;
    }

    // ── Exercise 03-A: Identical writes to both table types ─────────────────
    static void exerciseA(SparkSession spark) {
        System.out.println("\n━━━ Exercise 03-A: Write to CoW and MoR ━━━");

        List<Row> initialData = Arrays.asList(
                RowFactory.create("PCL-MM-001","TXN-001","PURCHASE", "Makati",      4_500_000.0,"PENDING","2024-01-15T09:00:00"),
                RowFactory.create("PCL-MM-002","TXN-002","MORTGAGE", "Makati",      3_200_000.0,"PENDING","2024-01-15T09:01:00"),
                RowFactory.create("PCL-QC-001","TXN-003","TRANSFER", "Quezon City", 2_800_000.0,"PENDING","2024-01-15T09:02:00"),
                RowFactory.create("PCL-QC-002","TXN-004","PURCHASE", "Quezon City", 8_100_000.0,"PENDING","2024-01-15T09:03:00"),
                RowFactory.create("PCL-BGC-001","TXN-005","LEASE",   "BGC",         6_500_000.0,"PENDING","2024-01-15T09:04:00")
        );
        Dataset<Row> df = spark.createDataFrame(initialData, schema());

        // Write to CoW
        System.out.println("Writing to CoW table...");
        long cowStart = System.currentTimeMillis();
        df.write().format("hudi")
                .options(baseOptions("COPY_ON_WRITE", "cow_table"))
                .mode("overwrite").save(COW_PATH);
        long cowTime = System.currentTimeMillis() - cowStart;

        // Write identical data to MoR
        System.out.println("Writing to MoR table...");
        long morStart = System.currentTimeMillis();
        df.write().format("hudi")
                .options(baseOptions("MERGE_ON_READ", "mor_table"))
                .mode("overwrite").save(MOR_PATH);
        long morTime = System.currentTimeMillis() - morStart;

        System.out.printf("%n Initial write timing:%n");
        System.out.printf("  CoW: %d ms%n", cowTime);
        System.out.printf("  MoR: %d ms%n", morTime);
        System.out.println("  (First writes are similar — delta appears on updates)\n");

        showFileStructure("CoW after initial write", COW_PATH);
        showFileStructure("MoR after initial write", MOR_PATH);

        System.out.println("OBSERVE:");
        System.out.println("  CoW: only .parquet base files — no log files");
        System.out.println("  MoR: also only .parquet base files (first write is the same)");
        System.out.println("  The difference appears when we write UPDATES next.\n");
    }

    // ── Exercise 03-B: Updates — where CoW and MoR diverge ─────────────────
    static void exerciseB(SparkSession spark) {
        System.out.println("\n━━━ Exercise 03-B: Updates — see CoW vs MoR diverge ━━━");
        System.out.println("Sending 3 update batches. Watch the file structure change.\n");

        for (int batch = 1; batch <= 3; batch++) {
            List<Row> updates = Arrays.asList(
                    RowFactory.create("PCL-MM-001","TXN-001","PURCHASE","Makati",
                            4_500_000.0, "VALID", "2024-01-15T1" + batch + ":00:00"),
                    RowFactory.create("PCL-QC-001","TXN-003","TRANSFER","Quezon City",
                            2_800_000.0, "INVALID", "2024-01-15T1" + batch + ":01:00")
            );
            Dataset<Row> df = spark.createDataFrame(updates, schema());

            System.out.println("--- Update batch " + batch + " ---");

            long cowStart = System.currentTimeMillis();
            df.write().format("hudi")
                    .options(baseOptions("COPY_ON_WRITE", "cow_table"))
                    .mode("append").save(COW_PATH);
            long cowTime = System.currentTimeMillis() - cowStart;

            long morStart = System.currentTimeMillis();
            df.write().format("hudi")
                    .options(baseOptions("MERGE_ON_READ", "mor_table"))
                    .mode("append").save(MOR_PATH);
            long morTime = System.currentTimeMillis() - morStart;

            System.out.printf("  Write time → CoW: %d ms | MoR: %d ms%n", cowTime, morTime);
        }

        System.out.println("\nAfter 3 update batches:\n");
        showFileStructure("CoW after 3 updates", COW_PATH);
        showFileStructure("MoR after 3 updates", MOR_PATH);

        System.out.println("KEY OBSERVATIONS:");
        System.out.println("  CoW: each update REWROTE the Parquet base file");
        System.out.println("       → only 1 current .parquet file per file group");
        System.out.println("       → writes are slower (read + merge + write)");
        System.out.println();
        System.out.println("  MoR: each update APPENDED to a .log file");
        System.out.println("       → 1 .parquet base file + 3 .log files per file group");
        System.out.println("       → writes are fast (append only)");
        System.out.println("       → reads must merge base + logs on the fly\n");
    }

    // ── Exercise 03-C: Read performance comparison ─────────────────────────
    static void exerciseC(SparkSession spark) {
        System.out.println("\n━━━ Exercise 03-C: Read performance comparison ━━━");

        // Snapshot read from CoW
        System.out.println("Snapshot read from CoW:");
        long cowReadStart = System.currentTimeMillis();
        long cowCount = spark.read().format("hudi").load(COW_PATH).count();
        long cowReadTime = System.currentTimeMillis() - cowReadStart;

        // Snapshot read from MoR (must merge base + logs)
        System.out.println("Snapshot read from MoR (merges base + log files):");
        long morReadStart = System.currentTimeMillis();
        long morCount = spark.read().format("hudi").load(MOR_PATH).count();
        long morReadTime = System.currentTimeMillis() - morReadStart;

        System.out.printf("%nRead timing (snapshot query):%n");
        System.out.printf("  CoW: %d ms — reads clean Parquet file%n", cowReadTime);
        System.out.printf("  MoR: %d ms — merges base + 3 log files%n", morReadTime);
        System.out.println("  MoR reads slower until compaction runs\n");

        // MoR read-optimised (base file only, skip logs)
        System.out.println("MoR read-optimised query (base file only, skips log files):");
        long roStart = System.currentTimeMillis();
        spark.read()
                .format("hudi")
                .option("hoodie.datasource.query.type", "read_optimized")
                .load(MOR_PATH)
                .select("parcelId", "validationStatus")
                .show(false);
        long roTime = System.currentTimeMillis() - roStart;

        System.out.printf("  RO query: %d ms — fast but returns STALE data (PENDING, not VALID)%n%n", roTime);
        System.out.println("OBSERVE the RO query returns validationStatus = PENDING");
        System.out.println("because it reads the original base file, skipping the log files.");
        System.out.println("This is the trade-off: speed vs freshness.\n");
    }

    // ── Exercise 03-D: Compaction — restoring MoR read performance ─────────
    static void exerciseD(SparkSession spark) {
        System.out.println("\n━━━ Exercise 03-D: Manual compaction on MoR ━━━");
        System.out.println("Compaction merges all log files back into the base file.");
        System.out.println("After compaction, MoR reads are as fast as CoW.\n");

        System.out.println("File structure BEFORE compaction:");
        showFileStructure("MoR before compaction", MOR_PATH);

        // Run compaction
        System.out.println("Running compaction...");
        long compactStart = System.currentTimeMillis();

        org.apache.hudi.client.SparkRDDWriteClient<org.apache.hudi.common.model.HoodieRecordPayload>
                client = getWriteClient(spark, MOR_PATH);
        String compactionInstant = client.scheduleCompaction(Option.empty()).get().toString();
        client.compact(compactionInstant);
        client.close();

        long compactTime = System.currentTimeMillis() - compactStart;
        System.out.printf("Compaction complete in %d ms%n%n", compactTime);

        System.out.println("File structure AFTER compaction:");
        showFileStructure("MoR after compaction", MOR_PATH);

        // Read after compaction
        System.out.println("Read after compaction:");
        long postCompactStart = System.currentTimeMillis();
        spark.read().format("hudi").load(MOR_PATH)
                .select("parcelId", "validationStatus", "_hoodie_commit_time")
                .show(false);
        long postCompactTime = System.currentTimeMillis() - postCompactStart;
        System.out.printf("Post-compaction read: %d ms (fast again, no log merging)%n%n", postCompactTime);

        System.out.println("KEY OBSERVATIONS:");
        System.out.println("  → Log files are gone — merged into new base Parquet file");
        System.out.println("  → Read time is now similar to CoW");
        System.out.println("  → validationStatus now correctly shows VALID/INVALID (from logs)");
        System.out.println("  → The compaction commit appears in the timeline as a .commit\n");
    }

    // ── Helper: show file structure ────────────────────────────────────────
    static void showFileStructure(String label, String basePath) {
        System.out.println(label + ":");
        try {
            org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(
                    new java.net.URI(basePath),
                    new org.apache.hadoop.conf.Configuration()
            );
            showFiles(fs, new org.apache.hadoop.fs.Path(basePath), "  ");
        } catch (Exception e) {
            System.out.println("  (Could not list files: " + e.getMessage() + ")");
        }
        System.out.println();
    }

    static void showFiles(org.apache.hadoop.fs.FileSystem fs,
                          org.apache.hadoop.fs.Path path,
                          String indent) throws Exception {
        for (org.apache.hadoop.fs.FileStatus status : fs.listStatus(path)) {
            String name = status.getPath().getName();
            if (name.startsWith(".hoodie")) continue; // skip metadata dir
            if (status.isDirectory()) {
                System.out.println(indent + name + "/");
                showFiles(fs, status.getPath(), indent + "  ");
            } else {
                String type = name.endsWith(".parquet") ? "[PARQUET]"
                        : name.contains(".log")     ? "[LOG    ]"
                          : "[OTHER  ]";
                System.out.printf("%s%s %s (%d KB)%n",
                        indent, type, name, status.getLen() / 1024);
            }
        }
    }

    static org.apache.hudi.client.SparkRDDWriteClient getWriteClient(
            SparkSession spark, String basePath) {

        // Minimal schema string — compaction doesn't need the actual data schema
        String emptySchema = "{\"type\":\"record\",\"name\":\"hoodie_source\","
                + "\"fields\":[{\"name\":\"_row_key\",\"type\":\"string\"}]}";

        org.apache.hudi.config.HoodieWriteConfig config =
                org.apache.hudi.config.HoodieWriteConfig.newBuilder()
                        .withPath(basePath)
                        .withSchema(emptySchema)
                        .withParallelism(2, 2)
                        .forTable("mor_table")
                        .build();

        return new org.apache.hudi.client.SparkRDDWriteClient<>(
                new org.apache.hudi.client.common.HoodieSparkEngineContext(
                        new org.apache.spark.api.java.JavaSparkContext(spark.sparkContext())),
                config
        );
    }

    // ── Main ───────────────────────────────────────────────────────────────
    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkConfig.createSession("Lab03-CowVsMor");

        exerciseA(spark);  // Initial writes to both
        exerciseB(spark);  // Updates — watch divergence
        exerciseC(spark);  // Read performance comparison
        exerciseD(spark);  // Compaction restores MoR performance

        // Summary
        System.out.println("\n━━━ Lab 03 Summary ━━━");
        System.out.println("CoW:  slow writes (rewrites files) → always fast reads");
        System.out.println("MoR:  fast writes (appends logs)   → slow reads until compaction");
        System.out.println("      RO query skips logs for speed but returns stale data");
        System.out.println("      Compaction periodically pays the read debt");
        System.out.println();
        System.out.println("For Beacon Phase 1 (POC): use CoW — simpler, no compaction needed");
        System.out.println("For Beacon Phase 2 (prod): MoR when write throughput becomes the limit");

        spark.stop();
    }
}