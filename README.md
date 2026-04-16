# PySpark ETL Pipeline

## Overview

A clean and scalable **ETL pipeline built using PySpark** that extracts product data from an API, transforms it, and loads it into Parquet format for analytical use.

---

##  Tech Stack

* Python
* PySpark
* REST API (requests)
* Parquet
* Git

---

## Project Structure

```
extract.py      # Data extraction from API
transform.py    # Data transformation logic
load.py         # Data loading (Parquet)
main.py         # Pipeline orchestration
requirements.txt
```

---

##  Workflow

### Extract

* Fetch data from API
* Convert JSON to Spark DataFrame

### Transform

* Type casting (price, rating, discount, stock)
* Remove unnecessary columns
* Handle null values
* Rename columns
* Create `final_price`

### Load

* Save processed data in **Parquet format**
* Optimized for performance and storage

---

##  Run the Pipeline

```bash
pip install -r requirements.txt
python main.py
```

---

##  Output

* Stored in: `data/processed/products/`
* Format: **Parquet**

---

##  Key Features

- Modular ETL architecture (Extract, Transform, Load separation)
- type casting
- Null value filtering and data cleaning
- Derived column creation (final_price)
- Efficient storage using Parquet format
