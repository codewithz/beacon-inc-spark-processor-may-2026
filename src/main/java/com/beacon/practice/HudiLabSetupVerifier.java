package com.beacon.practice;



import com.beacon.config.SparkConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public class HudiLabSetupVerifier {

    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkConfig.createSession("HudiSetupVerifier");

        System.out.println("\n=== Hudi Setup Verification ===");
        System.out.println("Spark version  : " + spark.version());
        System.out.println("Hudi catalog   : " +
                spark.conf().get("spark.sql.catalog.spark_catalog"));
        System.out.println("Serializer     : " +
                spark.conf().get("spark.serializer"));

        // Write a tiny Hudi table to confirm everything works
        List<Row> data = Arrays.asList(
                org.apache.spark.sql.RowFactory.create("PCL-VERIFY-001", "PURCHASE", 1000000.0),
                org.apache.spark.sql.RowFactory.create("PCL-VERIFY-002", "TRANSFER", 2000000.0)
        );

        org.apache.spark.sql.types.StructType schema = new org.apache.spark.sql.types.StructType()
                .add("parcelId",          org.apache.spark.sql.types.DataTypes.StringType)
                .add("transactionType",   org.apache.spark.sql.types.DataTypes.StringType)
                .add("transactionAmount", org.apache.spark.sql.types.DataTypes.DoubleType);

        Dataset<Row> df = spark.createDataFrame(data, schema);

        java.util.Map<String, String> options = new java.util.HashMap<>();
        options.put("hoodie.table.name",                              "verify_table");
        options.put("hoodie.datasource.write.recordkey.field",        "parcelId");
        options.put("hoodie.datasource.write.partitionpath.field",    "transactionType");
        options.put("hoodie.datasource.write.precombine.field",       "transactionAmount");
        options.put("hoodie.datasource.write.operation",              "upsert");
        options.put("hoodie.datasource.write.table.type",             "COPY_ON_WRITE");

        df.write()
                .format("hudi")
                .options(options)
                .mode("overwrite")
                .save("file:///C:/hudi/labs/verify");

        // Read it back
        Dataset<Row> result = spark.read()
                .format("hudi")
                .load("file:///C:/hudi/labs/verify");

        System.out.println("\n✅ Hudi write succeeded. Table contents:");
        result.select("parcelId", "transactionType", "transactionAmount",
                "_hoodie_commit_time").show(false);

        System.out.println("=== Setup complete — proceed to Lab 01 ===\n");
        spark.stop();
    }
}
