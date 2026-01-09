# 📊 Financial Data ETL Pipeline (Monthly Automated)

## 📌 Project Overview

This project implements a **fully automated monthly ETL (Extract, Transform, Load) pipeline** for financial transaction data. The pipeline ingests new data every month, performs comprehensive data cleaning and quality validation, loads the processed data into a SQL Server database, and automatically notifies stakeholders about any detected data quality or bias-related issues via email.

The entire workflow is **scheduled using Windows Task Scheduler**, ensuring zero manual intervention while preventing data redundancy and duplicate data loads.

---

## 🎯 Key Objectives

* Automate monthly ingestion of financial data
* Ensure **high data quality, consistency, and reliability**
* Prevent **duplicate or redundant records** through incremental loading
* Maintain transparency by proactively notifying stakeholders of data issues
* Build a scalable, production-ready ETL workflow using Python and SQL

---

## 🏗️ Architecture Overview

**Monthly Workflow:**

1. New financial data arrives as a monthly batch
2. Python ETL script processes raw data
3. Data quality and anomaly checks are executed
4. Cleaned data is incrementally loaded into SQL Server
5. A data quality report is generated
6. Automated email notifications are sent to stakeholders
7. Windows Task Scheduler triggers the pipeline every month

---

## ⚙️ Technologies Used

* **Python** – Core ETL logic, automation, and email alerts
* **SQL Server (SSMS)** – Data storage and incremental loading
* **Pandas & NumPy** – Data cleaning and transformation
* **pyodbc** – Python-to-SQL connectivity
* **Jupyter Notebook (.ipynb)** – Incremental monthly data loading
* **Windows Task Scheduler** – End-to-end monthly automation
* **SMTP (Email Automation)** – Stakeholder notifications

---

## 📂 Project Structure

```
📁 Financial_ETL_Pipeline
│
├── Financial_data_anomalies_Check.py   # Phase 1: Extraction, cleaning, and data quality checks
├── automate.ipynb                     # Phase 2: Incremental monthly load (no redundancy)
├── sql_scripts/                       # SQL scripts to create tables
├── send_email.py                      # Automated monthly email notifications
├── DQ_ISSUE.csv                       # Attached CSV containing data quality issues
└── README.md                          # Project documentation
```

---

## 🔄 ETL Process Details

### 1️⃣ Extraction

* Monthly financial data is ingested from source files
* Schema validation is performed to ensure structural consistency

### 2️⃣ Transformation & Cleaning (Python Script)

* Handling missing and null values
* Data type standardization
* Duplicate record detection
* Business-rule validation
* Identification of biased, anomalous, or inconsistent records

### 3️⃣ Data Quality Checks

* Null and missing value analysis
* Duplicate record detection
* Invalid data range validation
* Schema mismatch detection
* Bias and anomaly indicator checks

All identified issues are logged and stored for reporting and auditing purposes.

### 4️⃣ Incremental Load (Jupyter Notebook)

* Only **new monthly records** are inserted into SQL Server
* Control logic ensures prevention of redundancy
* Historical data remains intact and unchanged

### 5️⃣ Load into SQL Server

* Cleaned and validated data is loaded into production tables
* Control tables track the last successful load for incremental processing

---

## 📧 Automated Email Notification

* After each successful monthly update, a **data quality report** is generated
* Python automation sends an email to stakeholders and data owners
* The email includes:

  * Summary of newly added records
  * Detected data quality issues
  * Bias or anomaly indicators
  * Recommended action points for correction

This approach ensures transparency, accountability, and faster issue resolution.

---

## ⏱️ Automation Using Windows Task Scheduler

* The ETL pipeline is scheduled to run **monthly** using Windows Task Scheduler
* Automatically executes Python scripts and notebooks
* Eliminates repetitive manual execution
* Ensures timely, reliable, and consistent data updates

---

## ✅ Key Features

* Fully automated monthly ETL pipeline
* Incremental data loading with zero duplication
* Robust and extensible data quality framework
* Automated stakeholder communication
* Production-oriented and scalable architecture
* Reusable and modular ETL design

---

## 🚀 Future Enhancements

* Integration with cloud platforms (AWS / Azure)
* ETL monitoring and reporting dashboard using Power BI
* Advanced anomaly detection using machine learning
* Centralized logging, alerting, and failure recovery mechanisms

---

## 👤 Author

**Bharat Sharma**  
Data Analyst | Data Science Enthusiast

---

## 📌 Conclusion

This project demonstrates a real-world, production-grade ETL solution with a strong focus on **automation, data quality, scalability, and stakeholder communication**. It closely aligns with modern data engineering and analytics best practices.

---

## 📧 Automated Stakeholder Email Alert (Monthly)

As part of the ETL pipeline, an automated email is generated **every month** after the new financial data is processed. 
This email is sent to stakeholders and data owners, summarizing data quality issues, anomalies, and action points for correction.

Below is a sample of the automated email sent:

![Automated Data Quality Email Alert](https://github.com/user-attachments/assets/15debe80-1123-4c14-9008-438f990b61db)

*This email is triggered automatically each month via Windows Task Scheduler following the ETL run, ensuring timely communication of data quality insights to stakeholders.*

