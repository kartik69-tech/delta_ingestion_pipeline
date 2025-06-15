from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from delta import configure_spark_with_delta_pip
from faker import Faker
import pandas as pd
import time
import schedule
import smtplib
from email.mime.text import MIMEText

# Step 1: Spark Session with Delta support
builder = SparkSession.builder.appName("DeltaIngest") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Step 2: Fake Data Generator
faker = Faker()

def create_fake_data(n):
    rows = [(faker.name(), faker.address().replace('\n', ', '), faker.email()) for _ in range(n)]
    df = spark.createDataFrame(rows, ["Name", "Address", "Email"])
    df = df.withColumn("ingested_at", current_timestamp())
    return df

# Step 3: Delta Table Path
delta_path = "delta-table"  # saved in local folder

# Step 4: Append to Delta Table
def append_data(n=5):
    df = create_fake_data(n)
    df.write.format("delta").mode("append").save(delta_path)
    print("✅ New data added:")
    df.show(truncate=False)
    send_email_summary(df)

# Step 5: Send Email (configure below)python delta_ingestion.py

def send_email_summary(df):
    sender = "mathurkartik03@gmail.com"
    password = "jtug pvtw fkkg mdqd"
    recipient = "mathurkartik03@gmail.com"

    html = pd.DataFrame(df.collect()).to_html(index=False)
    msg = MIMEText(f"<h2>New Data Appended</h2>{html}", "html")
    msg["Subject"] = "Delta Pipeline Update"
    msg["From"] = sender
    msg["To"] = recipient

    try:
        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login(sender, password)
            server.sendmail(sender, recipient, msg.as_string())
        print("📧 Email sent.")
    except Exception as e:
        print(f"❌ Failed to send email: {e}")

# Step 6: Schedule Job
schedule.every(5).minutes.do(lambda: append_data(5))  # 5 rows every 5 minutes

print("📅 Running scheduled data ingestion every 5 minutes...")
append_data(5)  # run once at start

while True:
    schedule.run_pending()
    time.sleep(1)
