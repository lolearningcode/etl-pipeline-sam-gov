# 🛡️ SAM.gov ETL Pipeline for Veteran-Owned Contract Opportunities

This project is a **fully automated, serverless, containerized ETL pipeline** that fetches government contract opportunities from [SAM.gov](https://sam.gov/), filters for **veteran-owned business set-asides**, and makes the data instantly queryable through **AWS Athena**.

---

## 🔧 What It Does

- 📥 Fetches live federal contract data from the SAM.gov API
- 🧹 Transforms and ranks data using Python, Pandas, and PyArrow
- 💾 Stores cleaned `.parquet` datasets in Amazon S3
- 🧠 Automatically catalogs data with AWS Glue Crawlers
- 🔍 Enables SQL-based analysis through Amazon Athena
- 🐳 Built with Docker and deployed to AWS Lambda via ECR
- 🔁 Runs daily via EventBridge (CloudWatch Scheduler)

---

## 🚀 Tech Stack

| Category         | Tools / Services Used                                |
|------------------|-------------------------------------------------------|
| Programming      | Python 3.13, Pandas, PyArrow, Boto3                   |
| Cloud Platform   | AWS (Lambda, ECR, S3, Athena, Glue, EventBridge)      |
| Containerization | Docker (linux/amd64), Buildx, Custom Lambda Layers    |
| Infrastructure   | IAM, VPC awareness, Serverless architecture           |
| Data Format      | `.parquet` files optimized for Athena                 |
| Scheduling       | EventBridge (rate-based triggers)                     |

---

## 📦 Architecture Overview

1. **Data Ingestion**  
   - Calls the SAM.gov API with filters for veteran-owned opportunities  
   - Handles pagination, error responses, and query limits

2. **Transformation**  
   - Converts JSON to flattened DataFrame  
   - Adds `recencyScore`, NAICS mappings, and quality filters  
   - Exports to `.parquet` with PyArrow

3. **Storage & Analytics**  
   - Uploads to S3 using Boto3  
   - Glue Crawler updates schema for Athena  
   - Athena query (SQL) ranks top new opportunities

4. **Deployment**  
   - Lambda containerized with Docker + ECR  
   - EventBridge trigger runs it on a schedule  
   - CloudWatch logs all output for debugging & audit

---

## 📊 Example SQL Query (Athena)

```sql
SELECT title, solicitationNumber, postedDate, setAside, recencyScore
FROM contracts
WHERE recencyScore >= 4
ORDER BY postedDate DESC
LIMIT 10;