DROP TABLE IF EXISTS sample_table;
CREATE TABLE sample_table (
    id SERIAL PRIMARY KEY,
    int_col INTEGER UNIQUE,
    bigint_col BIGINT,
    smallint_col SMALLINT,
    varchar_col VARCHAR(50) NOT NULL,
    char_col CHAR(10),
    text_col TEXT,
    date_col DATE,
    timestamp_col TIMESTAMP,
    boolean_col BOOLEAN,
    numeric_col NUMERIC(10, 2),
    float_col FLOAT,
    fixed_col VARCHAR(50) DEFAULT 'fixed_value'
);