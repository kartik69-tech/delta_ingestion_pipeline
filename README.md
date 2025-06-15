# Delta Ingestion Pipeline

This project implements a Spark + Delta Lake ingestion pipeline that:
- Generates and appends fake user data
- Tracks Delta table versions
- Sends HTML summary emails after ingestion
- Schedules jobs at configurable intervals
