# 🧪 Delta Lake Ingestion Pipeline

This is a Spark + Delta Lake data ingestion pipeline that runs locally.

---

## Features

-  Fake Data Generation  
  Automatically creates synthetic user data (`Name`, `Address`, `Email`).

-  Delta Table Integration
  Uses the Delta Lake API to write, read, and version Delta tables locally.

-  Incremental Appending
  Configurable number of rows appended at each run (e.g., every 5 minutes).

-  Version Tracking 
  Automatically retrieves and prints the latest version of the Delta table.

-  Timezone Aware Timestamps  
  Stores data with timezone-correct ingestion timestamps.

-  Automated Scheduling  
  Uses `schedule` library to run ingestion every 5 minutes.

-  Email Notification  
  Sends HTML summary of new data to your inbox after each ingestion.

---

## Dependencies

Install required packages using:

pip install -r requirements.txt

---

## 📷 Sample Output

### ✅ Terminal Output

![Delta Table Output](Images/output_table.png)

### 📧 Email Notification

![Email Preview](Images/email_notification.png)
