CREATE DATABASE FinGuardDB;
GO
USE FinGuardDB;
GO


CREATE TABLE stg_transactions_raw (
    RowNum INT,
    trans_date_trans_time DATETIME,
    cc_num VARCHAR(25),
    merchant VARCHAR(255),
    category VARCHAR(50),
    amt FLOAT,
    gender CHAR(1),
    city VARCHAR(100),
    state CHAR(2),
    zip VARCHAR(10),
    lat FLOAT,
    long FLOAT,
    merch_lat FLOAT,
    merch_long FLOAT,
    is_fraud INT,
    LoadDate DATETIME DEFAULT GETDATE()
);

-- CLEAN TABLE
CREATE TABLE fact_transactions (
    TransactionID INT IDENTITY(1,1),
    trans_date_trans_time DATETIME,
    cc_num VARCHAR(25),
    merchant VARCHAR(255),
    category VARCHAR(50),
    amt FLOAT,
    gender CHAR(1),
    city VARCHAR(100),
    state CHAR(2),
    is_fraud INT,
    LoadDate DATETIME
);

-- DATA QUALITY + ANOMALY TABLE
CREATE TABLE dq_issues (
    IssueID INT IDENTITY(1,1),
    RowNum INT,
    ColumnName VARCHAR(50),
    IssueType VARCHAR(100),
    IssueDescription VARCHAR(255),
    DetectedAt DATETIME DEFAULT GETDATE()
);



USE [FinGuardDB]
GO

/****** Object:  Table [dbo].[etl_load_control]    Script Date: 09-01-2026 11:56:16 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[etl_load_control](
	[TableName] [varchar](50) NOT NULL,
	[LastLoadedDate] [datetime] NULL,
PRIMARY KEY CLUSTERED 
(
	[TableName] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO


