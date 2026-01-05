# Personal Analytics Data Pipeline 📊

A distinct, end-to-end ETL (Extract, Transform, Load) pipeline that migrates raw habit-tracking data from Notion into a cloud data warehouse for long-term analysis.

The project demonstrates a modern "Small Data" engineering stack: extracting from REST APIs, transforming JSON into a Star Schema, and automating daily batch loads via CI/CD.

## 🏗 Architecture

**Source** (Notion) $\rightarrow$ **ETL Script** (Python) $\rightarrow$ **Warehouse** (Supabase/PostgreSQL) $\rightarrow$ **UI** (Streamlit)

* **Extract:** Pulls daily habit logs via the Notion API.
* **Transform:** Python (Pandas) cleans data and structures it into a Dimensional Model.
* **Load:** Inserts/Upserts data into PostgreSQL to ensure idempotency.
* **Orchestration:** GitHub Actions runs the pipeline daily at 06:00 UTC.
* **Visualization:** Streamlit dashboard for monitoring completion rates and consistency trends.

## 🛠 Tech Stack

* **Language:** Python 3.9
* **Data Processing:** Pandas, NumPy
* **Database:** PostgreSQL (Hosted on Supabase)
* **ORM/Connectivity:** SQLAlchemy, Psycopg2
* **Automation:** GitHub Actions (Cron Schedule)
* **Visualization:** Streamlit, Plotly
* **Environment:** Dotenv for local security, GitHub Secrets for cloud security.

## 📂 Data Model (Star Schema)

The raw nested JSON from Notion is normalized into a Star Schema to facilitate easier SQL queries and aggregation.

* **`fact_habits`**: The central table recording specific events (Who, What, When, Status).
    * *Columns:* `log_id` (PK), `date_id` (FK), `habit_id` (FK), `status`, `is_completed`.
* **`dim_date`**: Temporal dimension for time-series analysis.
    * *Columns:* `date_id` (PK), `date`, `day_name`, `month`, `is_weekend`.
* **`dim_habit`**: Contextual information about the habits being tracked.
    * *Columns:* `habit_id` (PK), `habit_name`, `category`.

## 🚀 Key Features

* **Idempotent Ingestion:** The pipeline uses an "Upsert" strategy. If the script runs twice on the same day, it updates existing records rather than creating duplicates.
* **Connection Pooling:** Utilizes Supabase's Transaction Pooler (Port 6543) to handle serverless connections efficiently.
* **Secure Credential Management:** No hardcoded passwords. Uses `python-dotenv` for local development and GitHub/Streamlit Secrets for production.
* **Automated Error Handling:** The pipeline includes validation checks to skip malformed data without crashing the entire batch.

## 💻 Setup & Installation

### Prerequisites
* Python 3.9+
* A Notion Integration Token
* A Supabase (PostgreSQL) Database

### 1. Clone the Repository
```bash
git clone [https://github.com/yourusername/personal-analytics.git](https://github.com/yourusername/personal-analytics.git)
cd personal-analytics
```

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

### 3. Configure Environment
Create a `.env` file in the root directory:
```bash
NOTION_TOKEN=your_notion_secret_key
DATABASE_ID=your_notion_database_id
DATABASE_URL=postgresql://user:pass@host:6543/postgres?sslmode=require
```

### 4. Run Locally
To trigger the ETL process manually:
```bash
python main.py
```
To view the dashboard:
```bash
python -m streamlit run dashboard.py
```

## 🔄 Automation
The pipeline is defined in `.github/workflows/daily_run.yml`.
* **Trigger:** Schedule (Cron: `0 6 * * *`)
* **Runner:** Ubuntu Latest
* **Steps:** Checkout Repo -> Setup Python -> Install Libs -> Run `main.py`

## 📈 Future Improvements
* **Data Quality Tests:** Implement Great Expectations to validate data before loading.
* **Expanded Sources:** Integrate Apple Health or Fitbit API data.
* **Dbt Integration:** Move transformation logic from Python to dbt for better SQL version control.

---
*Created by David Kankowski.*