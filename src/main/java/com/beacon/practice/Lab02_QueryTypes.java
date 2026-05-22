package com.beacon.practice;



import com.beacon.config.SparkConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import java.util.List;

public class Lab02_QueryTypes {

    static final String BASE_PATH = "file:///C:/hudi/labs/beacon_transactions";

    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkConfig.createSession("Lab02-QueryTypes");

        // Get all distinct commit times — we need them for time travel and incremental
        System.out.println("=== Commit history ===");
        Dataset<Row> commits = spark.read().format("hudi").load(BASE_PATH)
                .select("_hoodie_commit_time")
                .distinct()
                .orderBy("_hoodie_commit_time");
        commits.show(false);

        List<Row> commitList = commits.collectAsList();
        String commit1 = commitList.get(0).getString(0);  // earliest
        String commit2 = commitList.get(1).getString(0);  // middle
        String commit3 = commitList.get(2).getString(0);  // latest

        System.out.println("Commit 1 (batch 1 — 5 records): " + commit1);
        System.out.println("Commit 2 (batch 2 — 2 updates, 1 insert): " + commit2);
        System.out.println("Commit 3 (batch 3 — 1 update, 1 insert): " + commit3);
        System.out.println();

        snapshotQuery(spark);
        timeTravelQuery(spark, commit1, commit2);
        incrementalQuery(spark, commit2, commit3);

        spark.stop();
    }

    // ── Query Type 1: Snapshot ─────────────────────────────────────────────
    static void snapshotQuery(SparkSession spark) {
        System.out.println("\n━━━ Query Type 1: SNAPSHOT ━━━");
        System.out.println("Returns the latest version of every record.");
        System.out.println("This is the DEFAULT — no extra configuration needed.\n");

        spark.read()
                .format("hudi")
                .load(BASE_PATH)
                .select("parcelId", "area", "status", "validationStatus",
                        "transactionAmount", "_hoodie_commit_time")
                .orderBy("area", "parcelId")
                .show(20, false);

        System.out.println("OBSERVE:");
        System.out.println("  → 7 records total (5 original + 2 new inserts)");
        System.out.println("  → PCL-MM-001, PCL-QC-001, PCL-BGC-001 show their LATEST status");
        System.out.println("  → Each record has its most recent _hoodie_commit_time\n");
    }

    // ── Query Type 2: Time Travel ──────────────────────────────────────────
    static void timeTravelQuery(SparkSession spark, String commit1, String commit2) {
        System.out.println("\n━━━ Query Type 2: TIME TRAVEL ━━━");
        System.out.println("Returns the table's state at a specific historical commit.");
        System.out.println("Real use case: 'What did the data look like before the batch 2 updates?'\n");

        // Travel back to after commit 1, before commit 2
        System.out.println("Travelling back to: " + commit1 + " (after batch 1, before any updates)");
        Dataset<Row> pastState = spark.read()
                .format("hudi")
                .option("as.of.instant", commit1)
                .load(BASE_PATH)
                .select("parcelId", "area", "validationStatus", "_hoodie_commit_time")
                .orderBy("area", "parcelId");

        pastState.show(20, false);

        System.out.println("OBSERVE:");
        System.out.println("  → Only 5 records (PCL-TG-001 and PCL-PS-001 didn't exist yet)");
        System.out.println("  → ALL validationStatus = PENDING (before validation team ran)");
        System.out.println("  → PCL-BGC-001 shows PENDING (not yet VALID as of commit 1)\n");

        // Compare with current
        System.out.println("Comparing: past vs present for PCL-MM-001:");
        System.out.println("  Past  (commit 1): validationStatus = PENDING");
        System.out.println("  Now   (snapshot): validationStatus = VALID");
        System.out.println("  Diff: batch 2 changed it\n");

        // Travel to after commit 2
        System.out.println("Travelling to: " + commit2 + " (after batch 2)");
        spark.read()
                .format("hudi")
                .option("as.of.instant", commit2)
                .load(BASE_PATH)
                .select("parcelId", "area", "validationStatus", "_hoodie_commit_time")
                .orderBy("area", "parcelId")
                .show(20, false);

        System.out.println("OBSERVE:");
        System.out.println("  → 6 records (PCL-TG-001 now exists, PCL-PS-001 doesn't yet)");
        System.out.println("  → PCL-MM-001 = VALID, PCL-QC-001 = INVALID (updated in batch 2)");
        System.out.println("  → PCL-BGC-001 still PENDING (batch 3 hasn't run yet in this view)\n");
    }

    // ── Query Type 3: Incremental ──────────────────────────────────────────
    static void incrementalQuery(SparkSession spark, String commit2, String commit3) {
        System.out.println("\n━━━ Query Type 3: INCREMENTAL ━━━");
        System.out.println("Returns ONLY records that changed between two commit times.");
        System.out.println("Most powerful query type for building efficient data pipelines.\n");
        System.out.println("Scenario: a downstream Silver layer job ran after commit 2.");
        System.out.println("          It needs to know: what changed in commit 3?\n");

        Dataset<Row> changed = spark.read()
                .format("hudi")
                .option("hoodie.datasource.query.type",              "incremental")
                .option("hoodie.datasource.read.begin.instanttime",  commit2)
                .option("hoodie.datasource.read.end.instanttime",    commit3)
                .load(BASE_PATH)
                .select("parcelId", "area", "status", "validationStatus",
                        "transactionAmount", "_hoodie_commit_time");

        System.out.println("Records that changed between commit 2 and commit 3:");
        changed.show(20, false);
        System.out.println("Total changed records: " + changed.count());

        System.out.println("\nOBSERVE:");
        System.out.println("  → Only 2 records returned (not all 7)");
        System.out.println("  → PCL-BGC-001: batch 3 updated it to VALID");
        System.out.println("  → PCL-PS-001:  batch 3 added it as a new record");
        System.out.println("  → The 5 unchanged records are NOT scanned at all");
        System.out.println("  → This is why incremental queries are so efficient at scale\n");

        // Open-ended incremental: everything since commit 2
        System.out.println("Open-ended incremental (everything after commit 2, no end time):");
        spark.read()
                .format("hudi")
                .option("hoodie.datasource.query.type",              "incremental")
                .option("hoodie.datasource.read.begin.instanttime",  commit2)
                // no end.instanttime — returns everything up to latest
                .load(BASE_PATH)
                .select("parcelId", "validationStatus", "_hoodie_commit_time")
                .show(false);
    }
}
