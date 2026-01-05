# Personal Analytics Pipeline 📊

An automated ETL pipeline that extracts daily habits from Notion, transforms them into a Star Schema, and loads them into a generic cloud Data Warehouse (Supabase/PostgreSQL) for analysis.

## 🏗 Architecture
**Extract** -> **Transform** -> **Load** -> **Automate**

* **Source:** Notion API (Raw JSON)
* **Transformation:** Python (Pandas)
    * Cleaning & Validation
    * Star Schema Modeling (`dim_date`, `dim_habit`, `fact_habits`)
* **Destination:** Supabase (PostgreSQL)
* **Orchestration:** GitHub Actions (Scheduled Daily Cron)

## 🛠 Tech Stack
* **Language:** Python 3.9
* **Libraries:** `pandas`, `sqlalchemy`, `requests`
* **Database:** PostgreSQL
* **CI/CD:** GitHub Actions

## 🚀 How it works
1.  The script runs automatically every morning via GitHub Actions.
2.  It fetches the latest habit logs from Notion.
3.  It calculates a composite key for the Date Dimension.
4.  It updates the Fact Table in the cloud database using an upsert strategy.