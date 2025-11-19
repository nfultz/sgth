# models/silver/users_stage.py
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
        pl.when((col("is_anomalous") == False) & (col("status_lower") == "active"))
          .then(lit('active'))

          # 2. Otherwise (if anomalous OR if clean but status is 'pending', 'inactive', etc.)
          .otherwise(lit('cancelled'))
          .alias("final_status"),

        # Standardize other text fields using Bronze's raw data
        col("first_name").str.strip_chars().str.to_titlecase().alias("first_name_cleaned"),
        col("last_name").str.strip_chars().str.to_titlecase().alias("last_name_cleaned"),
    )

# --- 2. NOT NULL CONSTRAINT IMPUTATION FIX ---
    final_clean_df = remediated_df.with_columns(
        # FIX: Impute 'Unknown' if first_name_cleaned is NULL or empty
        pl.when(col("first_name_cleaned").is_null() | (col("first_name_cleaned").str.len_chars() == 0))
          .then(lit('Unknown'))
          .otherwise(col("first_name_cleaned"))
          .alias("first_name_imputed"),

        # FIX: Also check last_name (since it's also NOT NULL)
        pl.when(col("last_name_cleaned").is_null() | (col("last_name_cleaned").str.len_chars() == 0))
          .then(lit('Unknown'))
          .otherwise(col("last_name_cleaned"))
          .alias("last_name_imputed"),

        # CRITICAL FIX: Impute phone number if it is NULL (meaning it failed validation in Bronze)
        pl.when(col("phone_int").is_null())
          .then(lit(-999999999).cast(pl.Int64)) # Use your sentinel value and ensure it's Int64
          .otherwise(col("phone_int"))
          .alias("phone_imputed"),
    )

    # --- 3. FINAL SELECTION for users_tmp ---

    # Select all columns with final names, ensuring no NULLs are left in NOT NULL fields
    users_tmp_df = final_clean_df.select(
        col("id_int").alias("id"),
        # Use imputed names
        col("first_name_imputed").alias("first_name"),
        col("last_name_imputed").alias("last_name"),
        # ... (rest of the columns)
        col("email_stripped").alias("email"),
        col("phone_imputed").alias("phone"),
        col("final_status").alias("status"),
        col("birth_date_dt").alias("birth_date"),
        col("created_at_dt").alias("created_at"),
    )


    # Return the clean and fully typed DataFrame
    return users_tmp_df
