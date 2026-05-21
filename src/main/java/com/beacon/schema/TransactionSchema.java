package com.beacon.schema;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

/**
 * Defines the schema that matches PropertyTransaction JSON exactly.
 * Spark uses this to parse the raw Kafka value bytes into typed columns.
 *
 * Field names must match the JSON keys produced by beacon-ingestion.
 */
public class TransactionSchema {

    public static StructType get() {
        return new StructType()
                .add("transactionId",     DataTypes.StringType,  true)
                .add("parcelId",          DataTypes.StringType,  true)
                .add("transactionType",   DataTypes.StringType,  true)
                .add("propertyType",      DataTypes.StringType,  true)
                .add("area",              DataTypes.StringType,  true)
                .add("city",              DataTypes.StringType,  true)
                .add("buyerName",         DataTypes.StringType,  true)
                .add("sellerName",        DataTypes.StringType,  true)
                .add("transactionAmount", DataTypes.DoubleType,  true)
                .add("currency",          DataTypes.StringType,  true)
                .add("titleDeedUri",      DataTypes.StringType,  true)
                .add("status",            DataTypes.StringType,  true)
                .add("validationStatus",  DataTypes.StringType,  true)
                .add("transactionDate",   DataTypes.StringType,  true); // keep as String, cast later
    }
}