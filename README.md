# End-to-End Data Engineering Pipeline (Databricks + Delta Lake)

## 📌 Overview

This project implements a production-style data pipeline using Databricks and Delta Lake following the Medallion Architecture (Bronze, Silver, Gold).

---

## ⚙️ Technologies Used

* Databricks (PySpark)
* Delta Lake
* SQL
* Window Functions
* Data Modeling (Fact & Dimension)

---

## 🧱 Architecture

* **Raw Layer**: Source data with ingestion timestamp
* **Bronze Layer**: Raw data stored as-is (append,overwrite)
* **Silver Layer**: Cleaned data with deduplication
* **Gold Layer**: Business-ready data (fact & dimension tables)

---

## 🔁 Incremental Processing

* Used `ingestion_time` as a watermark
* Filtered only new records
* Applied **MERGE (upsert)** to handle inserts and updates

---

## 🧹 Data Processing Steps

1. Ingest raw data and add ingestion timestamp
2. Store raw data in Bronze layer
3. Clean and deduplicate data in Silver layer using window functions
4. Perform incremental MERGE to update existing records
5. Build Gold layer:

   * Fact table with revenue calculation
   * Dimension table with surrogate keys

---

## 📊 Output

* Clean, deduplicated dataset
* Incrementally updated data
* Business-ready fact and dimension tables

---

## 🚀 Key Features

* Incremental data processing
* Watermark-based filtering
* Upsert logic using Delta MERGE
* Medallion architecture implementation

---

## 📷 Screenshots

##Bronze
<img width="921" height="309" alt="Bronze" src="https://github.com/user-attachments/assets/6b9ec576-9a0a-4471-b5a5-ec449d2a5ad1" />

## Silver
<img width="918" height="290" alt="Silver" src="https://github.com/user-attachments/assets/a70134ad-bb24-4fcd-9fd7-871504d3f468" />

## Gold-Dim
<img width="411" height="154" alt="gold_dim" src="https://github.com/user-attachments/assets/9849fc53-e538-409a-a1d8-626b9cdc6517" />

## Gold-Fact
<img width="893" height="273" alt="Gold_fact" src="https://github.com/user-attachments/assets/72ab92d9-d908-4ca2-8e07-73e73af69006" />


---

## 💬 Conclusion

This project demonstrates real-world data engineering concepts including incremental pipelines, Delta Lake operations, and dimensional modeling.
