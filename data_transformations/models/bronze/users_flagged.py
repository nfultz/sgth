# models/bronze/users_flagged.py
import dbt
import polars as pl
from datetime import date
from polars import col

## Requirements to check

# **Enrollment business rules:**
#
# * Users must be 18 years or older upon account creation.
# * Users must provide valid identifiers (email, phone number, birthdate) during enrollment.
# * Users should never be removed (i.e. rows deleted), they should be marked as cancelled.
# * Attached is a DB schema for storing the input, the data should conform completely to the schema.


def is_18yo(birth_date_dt, created_at_dt):
    """
    Polars expression: TRUE if the user was under 18 or if dates are invalid/missing.
    (i.e., this returns the ANOMALY flag).
    """
    # Calculate age in years at creation time
    age_at_creation = (created_at_dt.dt.year() - birth_date_dt.dt.year())

    return (
        birth_date_dt.is_null() |
        created_at_dt.is_null() |
        (age_at_creation < 18)
    )

def is_invalid_identifiers(email_stripped, phone_int, birth_date_dt):
    """
    Polars expression: TRUE if any required identifier is invalid/missing/unparsable.
    (i.e., this returns the ANOMALY flag).
    """
    return (
        # Birthdate is invalid if cast failed
        birth_date_dt.is_null() |
        # Email is invalid if NULL or empty after stripping
        email_stripped.is_null() | (email_stripped.str.len_chars() == 0) |
        # Phone is invalid if cast failed
        phone_int.is_null()
    )

def is_invalid_status(status_lower):
    """
    Polars expression: TRUE if the status is null/empty.
    (i.e., this returns the ANOMALY flag).
    """
    return (
        status_lower.is_null() | (status_lower.str.len_chars() == 0)
    )


# ==========================================================
# DBT MODEL FUNCTION
# ==========================================================

def model(dbt, session):
    # Reference the raw seed table
    raw_df = dbt.ref("challenge_dataset")
    df = pl.DataFrame(raw_df)

    # 1. TYPE CONVERSION & PRE-CLEANING STAGE
    typed_df = df.with_columns(
        # Safely cast and clean primary identifiers
        col("id").cast(pl.Int32, strict=False).alias("id_int"),
        col("phone").cast(pl.Int64, strict=False).alias("phone_int"),

        # Safely parse raw date strings to temporal types
        col("birth_date").str.strptime(pl.Date, format="%Y-%m-%d", strict=False).alias("birth_date_dt"),
        col("created_at").str.strptime(pl.Datetime, format="%Y-%m-%d %H:%M:%S", strict=False).alias("created_at_dt"),

        # Pre-clean status and email
        col("status").str.to_lowercase().alias("status_lower"),
        col("email").str.strip_chars().alias("email_stripped")
    )

    # 2. ANOMALY DETECTION STAGE (Using helper functions)
    flagged_df = typed_df.with_columns(
                # PREDICATE 1: Age check
                is_18yo(col("birth_date_dt"), col("created_at_dt")).alias("is_age_anomaly"),

                # PREDICATE 2: Identifier check
                is_invalid_identifiers(col("email_stripped"), col("phone_int"), col("birth_date_dt")).alias("is_identifier_anomaly"),

                # PREDICATE 3: Status check
                is_invalid_status(col("status_lower")).alias("is_status_anomaly"),
                )

    flagged_df = flagged_df.with_columns(
                (col("is_age_anomaly") | col("is_identifier_anomaly") | col("is_status_anomaly")).alias("is_anomalous")
            )

    # Return all columns (raw, typed, and flags) for downstream use
    return flagged_df.select(pl.all())

