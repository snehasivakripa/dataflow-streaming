# 🟢 Dataflow Streaming Application with Redis

A streaming data pipeline using **Apache Beam** that reads text files from Google Cloud Storage, transforms the data, and writes processed records to **Redis**. This application is built using **Spring Boot** and demonstrates a simple ETL pipeline for structured data streaming.

---

## 🧩 Features

- Reads files from a GCS bucket (`*.txt`) in a streaming fashion  
- Splits each line by the pipe (`|`) delimiter into fields  
- Maps fields to a structured format with keys like `guid`, `firstName`, `lastName`, etc.  
- Writes processed data into Redis using **SADD** method for quick lookups  
- Supports near real-time streaming by polling the bucket periodically  
- Logs each line processed for debugging purposes  

---

## 📄 Example Input File

Each line of the input file should follow this structure:
guid|firstName|middleName|lastName|dob|postalCode|gender|pid


Example:
123|John|A|Doe|1990-01-01|12345|M|987
---

## 🛠️ Tech Stack

**Backend / Pipeline**:

- Java 17+
- Spring Boot
- Apache Beam
- Apache Beam RedisIO
- Joda-Time (for duration management)
- SLF4J (logging)

**Database / Storage**:

- Redis (for storing key-value pairs)

**Cloud / Streaming**:

- Google Cloud Storage (for input files)

---

## ⚙️ Setup Instructions

### 1️⃣ Prerequisites

- Java 17 or higher  
- Apache Maven  
- Redis running locally (default: `localhost:6379`) or remotely  
- GCS bucket containing input files  

---

### 2️⃣ Clone the repository

```bash
git clone <your-repo-url>
cd DataflowStreaming
