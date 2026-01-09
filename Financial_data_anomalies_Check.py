import pandas as pd
def extract_data(file_path):
    df = pd.read_csv(file_path)

    # Preserve row number for traceability
    df.reset_index(inplace=True)
    df.rename(columns={"index": "RowNum"},inplace = True)

    df["trans_date_trans_time"] = pd.to_datetime(
        df["trans_date_trans_time"],
        errors="coerce"
    )

    return df

file_path = r"C:\Users\bhara\OneDrive\Desktop\Automated ETL & Data Quality Pipeline for Financial Transactions\fraudTrain.csv"
df = extract_data(file_path)

# ETL Task
import numpy as np

def detect_issues(df):
    issues = []

    # 1️⃣ NULL VALUE CHECK
    for col in df.columns:
        null_rows = df[df[col].isnull()]
        for _, row in null_rows.iterrows():
            issues.append((
                int(row["RowNum"]),
                col,
                "NULL_VALUE",
                "Null value detected"
            ))

    # 2️⃣ INVALID GENDER
    invalid_gender = df[~df["gender"].isin(["M", "F"])]
    for _, row in invalid_gender.iterrows():
        issues.append((
            int(row["RowNum"]),
            "gender",
            "INVALID_VALUE",
            "Gender must be M or F"
        ))

    # 3️⃣ INVALID FRAUD LABEL
    invalid_fraud = df[~df["is_fraud"].isin([0, 1])]
    for _, row in invalid_fraud.iterrows():
        issues.append((
            int(row["RowNum"]),
            "is_fraud",
            "INVALID_CLASS",
            "Fraud label must be 0 or 1"
        ))

    # 4️⃣ INVALID AMOUNT
    invalid_amt = df[df["amt"] <= 0]
    for _, row in invalid_amt.iterrows():
        issues.append((
            int(row["RowNum"]),
            "amt",
            "INVALID_AMOUNT",
            "Transaction amount must be greater than 0"
        ))

    # 5️⃣ OUTLIER DETECTION (IQR – FINANCIAL STANDARD)
    q1 = df["amt"].quantile(0.25)
    q3 = df["amt"].quantile(0.75)
    iqr = q3 - q1
    upper_limit = q3 + 1.5 * iqr

    outliers = df[df["amt"] > upper_limit]
    for _, row in outliers.iterrows():
        issues.append((
            int(row["RowNum"]),
            "amt",
            "OUTLIER",
            f"High transaction amount: {row['amt']}"
        ))

    # 6️⃣ DUPLICATE TRANSACTION CHECK
    duplicates = df[df.duplicated(subset=["cc_num", "unix_time", "amt"])]
    for _, row in duplicates.iterrows():
        issues.append((
            int(row["RowNum"]),
            "ALL",
            "DUPLICATE_RECORD",
            "Duplicate transaction detected"
        ))

    # 7️⃣ DATA DRIFT / CLASS IMBALANCE
    fraud_ratio = df["is_fraud"].mean()
    if fraud_ratio < 0.005:
        issues.append((
            -1,
            "is_fraud",
            "DATA_IMBALANCE",
            "Extreme class imbalance detected"
        ))

    return issues


def clean_data(df):
    df["amt"] = pd.to_numeric(df["amt"], errors="coerce")
    df["is_fraud"] = pd.to_numeric(df["is_fraud"], errors="coerce")
    return df


import pyodbc
DB_CONN_STR = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=localhost\\BHARAT_DUNGRIYA;"
    "DATABASE=FinGuardDB;"
    "Trusted_Connection=yes;"
)

def load_raw(df):
    conn = pyodbc.connect(DB_CONN_STR)
    cur = conn.cursor()

    for _, r in df.iterrows():
        cur.execute("""
            INSERT INTO stg_transactions_raw
            (RowNum, trans_date_trans_time, cc_num, merchant, category,
             amt, gender, city, state, unix_time, is_fraud)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        int(r["RowNum"]),
        r["trans_date_trans_time"],
        str(r["cc_num"]),
        r["merchant"],
        r["category"],
        float(r["amt"]),
        r["gender"],
        r["city"],
        r["state"],
        int(r["unix_time"]),
        int(r["is_fraud"])
        )

    conn.commit()
    conn.close()


def load_clean(df):
    conn = pyodbc.connect(DB_CONN_STR)
    cur = conn.cursor()

    for _, r in df.iterrows():
        cur.execute("""
            INSERT INTO fact_transactions
            (trans_date_trans_time, cc_num, merchant, category,
             amt, gender, city, state, is_fraud, LoadDate)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, GETDATE())
        """,
        r["trans_date_trans_time"],
        str(r["cc_num"]),
        r["merchant"],
        r["category"],
        float(r["amt"]),
        r["gender"],
        r["city"],
        r["state"],
        int(r["is_fraud"])
        )

    conn.commit()
    conn.close()


def log_issues(issues):
    conn = pyodbc.connect(DB_CONN_STR)
    cur = conn.cursor()

    for i in issues:
        cur.execute("""
            INSERT INTO dq_issues
            (RowNum, ColumnName, IssueType, IssueDescription)
            VALUES (?, ?, ?, ?)
        """,
        i[0], i[1], i[2], i[3])

    conn.commit()
    conn.close()

import pyodbc

DB_CONN_STR = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=BHARAT_DUNGRIYA\\SQLEXPRESS;"
    "DATABASE=FinGuardDB;"      # change if DB name is different
    "Trusted_Connection=yes;"
    "Encrypt=Optional;"
    "TrustServerCertificate=yes;"
)

conn = pyodbc.connect(DB_CONN_STR)
print("✅ Connected to SQL Server successfully!")
conn.close()



def load_raw(df, batch_size=1000):
    conn = pyodbc.connect(DB_CONN_STR)
    cur = conn.cursor()
    cur.fast_executemany = False   # 🔥 IMPORTANT FIX

    insert_sql = """
        INSERT INTO stg_transactions_raw
        (RowNum, trans_date_trans_time, cc_num, merchant, category,
         amt, gender, city, state, zip,
         lat, long, merch_lat, merch_long, is_fraud)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    for start in range(0, len(df), batch_size):
        batch = df.iloc[start:start+batch_size]

        data = [
            (
                int(r["RowNum"]),
                r["trans_date_trans_time"],
                str(r["cc_num"]),
                r["merchant"],
                r["category"],
                float(r["amt"]) if pd.notnull(r["amt"]) else None,
                r["gender"],
                r["city"],
                r["state"],
                str(r["zip"]),
                float(r["lat"]) if pd.notnull(r["lat"]) else None,
                float(r["long"]) if pd.notnull(r["long"]) else None,
                float(r["merch_lat"]) if pd.notnull(r["merch_lat"]) else None,
                float(r["merch_long"]) if pd.notnull(r["merch_long"]) else None,
                int(r["is_fraud"]) if pd.notnull(r["is_fraud"]) else None
            )
            for _, r in batch.iterrows()
        ]

        cur.executemany(insert_sql, data)
        conn.commit()

    conn.close()

df["trans_date_trans_time"] = pd.to_datetime(
    df["trans_date_trans_time"],
    errors="coerce"
)

load_raw(df)
total_rows = len(df)
print(f"Total rows to load: {total_rows}")


def log_issues(issues, batch_size=300):
    if not issues:
        return

    conn = pyodbc.connect(DB_CONN_STR)
    cur = conn.cursor()
    cur.fast_executemany = False  # 🔥 MUST BE FALSE

    insert_sql = """
        INSERT INTO dq_issues
        (RowNum, ColumnName, IssueType, IssueDescription)
        VALUES (?, ?, ?, ?)
    """

    for start in range(0, len(issues), batch_size):
        batch = issues[start:start + batch_size]
        cur.executemany(insert_sql, batch)
        conn.commit()

    conn.close()

    
def load_clean(df, batch_size=500):
    """
    Load cleaned data into fact_transactions table in batches
    to avoid OperationalError (08S01) on SQL Server.
    """
    conn = pyodbc.connect(DB_CONN_STR)
    cur = conn.cursor()
    cur.fast_executemany = False  # MUST be False for stability

    insert_sql = """
        INSERT INTO fact_transactions
        (trans_date_trans_time, cc_num, merchant, category,
         amt, gender, city, state, is_fraud, LoadDate)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, GETDATE())
    """

    for start in range(0, len(df), batch_size):
        batch = df.iloc[start:start + batch_size]

        data = []
        for _, r in batch.iterrows():
            data.append((
                r["trans_date_trans_time"],                # DATETIME
                str(r["cc_num"]),                          # VARCHAR
                r["merchant"],                             # VARCHAR
                r["category"],                             # VARCHAR
                float(r["amt"]) if pd.notnull(r["amt"]) else None,  # FLOAT
                r["gender"],                               # CHAR(1)
                r["city"],                                 # VARCHAR
                r["state"],                                # CHAR(2)
                int(r["is_fraud"]) if pd.notnull(r["is_fraud"]) else None  # INT
            ))

        cur.executemany(insert_sql, data)
        conn.commit()

    conn.close()


load_clean(df)
issues = detect_issues(df)
log_issues(issues)



def extract_incremental_data(file_path, last_loaded_date):
    df = pd.read_csv(file_path)

    df["trans_date_trans_time"] = pd.to_datetime(
        df["trans_date_trans_time"],
        errors="coerce"
    )

    df_new = df[df["trans_date_trans_time"] > last_loaded_date]

    df_new.reset_index(inplace=True)
    df_new.rename(columns={"index": "RowNum"}, inplace=True)

    return df_new




def update_last_loaded_date(df):
    max_date = df["trans_date_trans_time"].max()

    conn = pyodbc.connect(DB_CONN_STR)
    cur = conn.cursor()

    cur.execute("""
        UPDATE dbo.etl_load_control
        SET LastLoadedDate = ?
        WHERE TableName = 'fraud_transactions'
    """, max_date)

    conn.commit()
    conn.close()


def get_last_loaded_date():
    conn = pyodbc.connect(DB_CONN_STR)
    cur = conn.cursor()

    cur.execute("""
        IF NOT EXISTS (
            SELECT 1 FROM dbo.etl_load_control
            WHERE TableName = 'fraud_transactions'
        )
        INSERT INTO dbo.etl_load_control
        VALUES ('fraud_transactions', '1900-01-01')
    """)

    cur.execute("""
        SELECT LastLoadedDate
        FROM dbo.etl_load_control
        WHERE TableName = 'fraud_transactions'
    """)

    last_date = cur.fetchone()[0]
    conn.commit()
    conn.close()
    return last_date
