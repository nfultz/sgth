import dbt
import polars as pl
from polars import col, lit

def model(dbt, session):
    # 1. Reference the Bronze layer model containing the flags and raw data
    flagged_df = dbt.ref("users_flagged")
    df = pl.DataFrame(flagged_df)

    # 2. Filter for ONLY the anomalous records
    anomalous_records = df.filter(col("is_anomalous") == True)

    # 3. Select and log the required information
    log_df = anomalous_records.select(
        # Key Identifier
        col("id_int").alias("anomalous_user_id"),

        # Timestamp of when the record entered the system (for auditing)
        col("created_at_dt").alias("original_creation_date"),

        # Remediation Action (What was done to the row in users_new)
        lit("Status changed to 'cancelled' due to validation failure").alias("remediation_action"),

        # Specific Anomaly Reasons (To diagnose the source issue)
        col("is_age_anomaly"),
        col("is_identifier_anomaly"),
        col("is_status_anomaly"),

        # Optional: Log the original raw data for troubleshooting
        col("email").alias("raw_email"),
        col("phone").alias("raw_phone"),
    )

    # Return the log table
    return log_df

