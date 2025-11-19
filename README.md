# **Data Transformations Project (dbt \+ Polars)**

This project is built using dbt (Data Build Tool) with the DuckDB adapter and utilizes Polars for complex data cleaning and remediation tasks. The goal is to ingest raw user data and transform it into a highly standardized, quality-controlled Silver layer.

## **Project Layers**

This project follows a three-layer architecture (Bronze, Silver, and Reports) to ensure progressive data quality and structure enforcement.

### **1\. Bronze Layer (Raw Staging)**

The Bronze layer (models/bronze/) is responsible for initial data integrity checks, type casting, and identifying anomalies.

* **users\_old:** A simple SQL view referencing the raw source data.
* **users\_flagged.py (Polars):** The core Bronze model that performs initial transformations:
  * **Type Casting:** Ensures columns like id, phone, birth\_date, and created\_at are cast to correct numeric and date/time types.
  * **Data Standardization:** Converts fields like status and names to a standard format.
  * **Anomaly Flagging:** Adds columns (e.g., is\_anomalous) to flag records that contain invalid or suspicious values (like non-numeric phone numbers or missing dates).

### **2\. Silver Layer (Clean and Contracted)**

The Silver layer (models/silver/) contains the final, trusted data set. Its primary characteristic is the use of **Data Contracts** to strictly enforce the schema and data quality rules.

* **users\_tmp.py (Polars):** This staging model receives data from the Bronze layer and applies final remediation:
  * **NOT NULL Imputation:** Replaces NULL values in required columns (e.g., first\_name and last\_name become 'Unknown'; phone becomes \-999999999).
  * **Status Remediation:** Imputes records with anomalous or invalid statuses to the safe, final value of 'cancelled'.
* **users\_new.sql (Contracted Table):** This final SQL model simply selects all columns from users\_tmp. It is configured to enforce the **DDL Contract** defined in schema.yml.
  * **Schema Enforcement:** Ensures columns like id, first\_name, and phone are physically created in DuckDB with the NOT NULL constraint, preventing bad data from ever being written to the final table.
  * **Data Types:** Enforces precise data types, such as DATE for birth\_date and TIMESTAMP for created\_at.

## **Key Technical Decisions**

### **Python Materialization Compatibility**

To leverage the advanced data processing capabilities of Polars while maintaining the strict DDL enforcement of dbt, we adopted the following pattern:

1. **Python for Logic:** All complex data validation, cleaning, and imputation are handled in Python models (.py files).
2. **SQL for Contracts:** The final Silver model (users\_new.sql) is a simple SELECT \* query. This allows dbt's native contract enforcement engine to generate the clean DDL (CREATE TABLE ... NOT NULL ...) on the final table.
3. **Scoped Contract Enforcement:** Data Contracts are only enforced on the silver directory in dbt\_project.yml to avoid compilation errors with Python models in the Bronze layer.

## **How to Run the Project**

1. **Install Dependencies:** Ensure you have dbt, dbt-duckdb, and Polars installed in your environment.
2. **Run Transformations:** Execute the dbt run command to build all models:
   dbt run

3. **Inspect Schema:** To verify the final schema enforcement, use the following DuckDB command:
   duckdb dev.duckdb "describe table users\_new"

   This command should show **"NO"** under the null column for all required fields.

### **AI Usage**

[Google Gemini](https://gemini.google.com/share/a8797f7de67e) was choosen to assist on this, and we lived to regret it.
