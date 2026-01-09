import smtplib
from email.message import EmailMessage
import os

# ==========================
# EMAIL CONFIG
# ==========================
SENDER_EMAIL = "bharat31072007@gmail.com"
APP_PASSWORD = "XXXX YYYY UUUU NNNN"   # Gmail App Password (16 chars) this is dummy password
RECEIVER_EMAIL = "sharma.bharat0703@gmail.com"

# ==========================
# FILE PATH
# ==========================
CSV_FILE_PATH = r"C:\Users\bhara\OneDrive\Desktop\Automated ETL & Data Quality Pipeline for Financial Transactions\DQ_ISSUE.csv"

# ==========================
# CREATE EMAIL
# ==========================
msg = EmailMessage()
msg["From"] = SENDER_EMAIL
msg["To"] = RECEIVER_EMAIL
msg["Subject"] = "Monthly Financial Data Quality Report"

msg.set_content(
    "Hello,\n\n"
    "Here is the monthly update of data quality issues.\n"
    "Please check the attached file and correct the issues wherever required.\n\n"
    "Regards,\n"
    "Bharat"
)

# ==========================
# ATTACH CSV FILE
# ==========================
with open(CSV_FILE_PATH, "rb") as f:
    file_data = f.read()
    file_name = os.path.basename(CSV_FILE_PATH)

msg.add_attachment(
    file_data,
    maintype="application",
    subtype="octet-stream",
    filename=file_name
)

# ==========================
# SEND EMAIL
# ==========================
with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
    smtp.login(SENDER_EMAIL, APP_PASSWORD)
    smtp.send_message(msg)

print("✅ Email sent successfully with CSV attachment!")

 
