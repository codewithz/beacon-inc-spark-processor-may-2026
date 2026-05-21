CREATE TABLE IF NOT EXISTS property_transactions (
                                                     parcel_id           VARCHAR(50)     PRIMARY KEY,
    transaction_id      VARCHAR(50),
    transaction_type    VARCHAR(30),
    property_type       VARCHAR(30),
    area                VARCHAR(100),
    city                VARCHAR(100),
    buyer_name          VARCHAR(200),
    seller_name         VARCHAR(200),
    transaction_amount  NUMERIC(15,2),
    currency            VARCHAR(10),
    title_deed_uri      VARCHAR(500),
    status              VARCHAR(30),
    validation_status   VARCHAR(30),
    transaction_date    TIMESTAMP,
    kafka_partition     INT,
    kafka_offset        BIGINT,
    last_updated        TIMESTAMP DEFAULT NOW()
    );