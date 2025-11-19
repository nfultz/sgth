# models/silver/users_new.py
import dbt
import polars as pl
from polars import col, lit

def model(dbt, session):
    # Reference the Bronze layer model containing the flags and typed columns
    flagged_df = dbt.ref("users_flagged")
    df = pl.DataFrame(flagged_df)

    # --- 1. REMEDIATION AND CLEANING LOGIC ---

    remediated_df = df.with_columns(
        # CRITICAL: Apply the remediation rule:
        # If the record is anomalous, set status to 'cancelled'; otherwise, use the cleaned status string.
        # CRITICAL: Comprehensive Status Remediation
        # 1. Check if the record is CLEAN AND represents 'active'
        pl.when((col("is_anomalous") == False) & (col("status_lower").str == "active"))
          .then(lit('active'))

          # 2. Otherwise (if anomalous OR if clean but status is 'pending', 'inactive', etc.)
          .otherwise(lit('cancelled'))
          .alias("final_status"),

        # Standardize other text fields using Bronze's raw data
        col("first_name").str.strip_chars().str.to_titlecase().alias("first_name_cleaned"),
        col("last_name").str.strip_chars().str.to_titlecase().alias("last_name_cleaned"),
    )

    # --- 2. FINAL SELECTION AND TYPE ALIGNMENT ---

    # Select the columns needed for the target users_new schema.
    # We rely on the Bronze layer's success (id_int, birth_date_dt, phone_int, created_at_dt)
    # to provide the correctly typed data, and then apply final NOT NULL/ENUM logic.
    users_new_df = remediated_df.select(
        # Primary Key (INT NOT NULL)
        col("id_int").alias("id"),

        # Varchar (NOT NULL)
        col("first_name_cleaned").alias("first_name"),
        col("last_name_cleaned").alias("last_name"),
        col("email_stripped").alias("email"), # Use the cleaned/stripped email from Bronze

        # Int(10) (NOT NULL)
        col("phone_int").alias("phone"), # Use the safely casted integer from Bronze

        # ENUM/Varchar (NOT NULL) - Status is guaranteed to be 'cancelled' or a cleaned value
        pl.when(col("final_status") == 'active').then(lit('active'))
          .otherwise(lit('cancelled')) # Default to cancelled if not explicitly 'active' (handles all other raw values)
          .alias("status"),

        # Date (NOT NULL)
        col("birth_date_dt").alias("birth_date"), # Use the safely casted date from Bronze

        # Datetime (NOT NULL)
        col("created_at_dt").alias("created_at"), # Use the safely casted datetime from Bronze
    )

    # Return the clean and fully typed DataFrame
    return users_new_df
