# Customer-satisfaction-upgradation-Pyspark-Project

This project simulates real-time retail transaction analysis using PySpark in Databricks, S3, and PostgreSQL.



##  Mechanism X – Transaction Ingestion

- Pulls 10,000-record chunks from Google Drive
- Uploads to S3 every second (stream simulation)

### Mechanism Y – Pattern Detection Engine

- Monitors S3 for new chunks
- Detects customer patterns:
  - PatId1: High txn, low value → Upgrade
  - PatId2: Low avg value, many txns → Child
  - PatId3: Female underrepresentation → DEI-NEEDED
- Writes detections in batches of 50 to S3

####  Tech Stack

- PySpark (Databricks)
- AWS S3
- PostgreSQL (temp state store)


#####  Setup Instructions

1. Upload CustomerImportance.csv to DBFS
2. Run mechanism_x_ingestion.py to simulate chunk ingestion
3. Run mechanism_y_detection.py on Databricks cluster
4. Monitor S3 for detection output

###### Output Format

Each detection contains:

| YStartTime | detectionTime | patternId | ActionType | customerName | MerchantId |
|------------|----------------|-----------|------------|---------------|------------|

