package com.beacon.config;

public class PostgresConfig {

    public static final String URL      = System.getProperty("db.url",      "jdbc:postgresql://localhost:5432/beacon-inc");
    public static final String USER     = System.getProperty("db.user",     "postgres");
    public static final String PASSWORD = System.getProperty("db.password", "admin");
    public static final String DRIVER   = "org.postgresql.Driver";

    public static final String TABLE    = "property_transactions";
}
