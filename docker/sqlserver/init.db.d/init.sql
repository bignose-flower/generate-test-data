IF NOT EXISTS(SELECT * FROM sys.databases WHERE name = 'sample_db')
BEGIN
    CREATE DATABASE sample_db;
END
GO

USE sample_db;
GO

-- テーブルが存在しない場合のみ作成
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'sample_table' AND type = 'U')
BEGIN
    CREATE TABLE sample_table (
        id INT IDENTITY(1,1) PRIMARY KEY,
        int_col INT UNIQUE,
        bigint_col BIGINT,
        smallint_col SMALLINT,
        varchar_col VARCHAR(50) NOT NULL,
        char_col CHAR(10),
        text_col NVARCHAR(MAX),
        date_col DATE,
        timestamp_col DATETIME,
        boolean_col BIT,
        numeric_col DECIMAL(10, 2),
        float_col FLOAT,
        fixed_col VARCHAR(50) DEFAULT 'fixed_value'
    );
END
GO