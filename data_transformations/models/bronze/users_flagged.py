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


# Regex from SO: https://stackoverflow.com/a/201378/986793
EMAIL_REGEX = r"(?:[a-z0-9!#$%&'*+\x2f=?^_`\x7b-\x7d~\x2d]+(?:\.[a-z0-9!#$%&'*+\x2f=?^_`\x7b-\x7d~\x2d]+)*|\"(?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21\x23-\x5b\x5d-\x7f]|\\[\x01-\x09\x0b\x0c\x0e-\x7f])*\")@(?:(?:[a-z0-9](?:[a-z0-9\x2d]*[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9\x2d]*[a-z0-9])?|\[(?:(?:(2(5[0-5]|[0-4][0-9])|1[0-9][0-9]|[1-9]?[0-9]))\.){3}(?:(2(5[0-5]|[0-4][0-9])|1[0-9][0-9]|[1-9]?[0-9])|[a-z0-9\x2d]*[a-z0-9]:(?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21-\x5a\x53-\x7f]|\\[\x01-\x09\x0b\x0c\x0e-\x7f])+)\])"

def is_18yo(birth_date_dt, created_at_dt):
    """
    Polars expression: TRUE if the user was under 18 or if dates are invalid/missing.
    (i.e., this returns the ANOMALY flag).
    """
    # Calculate age in years at creation time, accounting for whether birthday has occurred
    age_at_creation = (
        created_at_dt.dt.year() - birth_date_dt.dt.year()
        - (
            (created_at_dt.dt.month() < birth_date_dt.dt.month())
            | (
                (created_at_dt.dt.month() == birth_date_dt.dt.month())
                & (created_at_dt.dt.day() < birth_date_dt.dt.day())
            )
        ).cast(pl.Int64)
    )

    return (
        birth_date_dt.is_null()
        | created_at_dt.is_null()
        | (age_at_creation < 18)
    )

def is_invalid_phone_math(phone_int):
    """
    Polars expression: TRUE if the phone integer fails length or prefix rules.
    This relies on the phone being successfully cast to an integer in the main model.
    """
    # 1. Check for missing/uncastable data
    is_missing_or_dirty = phone_int.is_null()

    # 2. Check for 10 digits exact length (10 digits means 10^9 <= number < 10^10)
    is_not_ten_digits = (phone_int < 10**9) | (phone_int >= 10**10)

    # 3. Check for restricted prefixes (0, 1, 555, 911)

    # Get the first digit (by dividing by 10^9)
    first_digit = (phone_int / 10**9).cast(pl.Int64)
    is_invalid_start = (first_digit == 0) | (first_digit == 1)

    # Get the first three digits (by dividing by 10^7)
    first_three_digits = (phone_int / 10**7).cast(pl.Int64)
    is_reserved_prefix_3 = (first_three_digits == 555) | (first_three_digits == 911)

    # The ANOMALY flag is TRUE if it's missing OR fails any of the rules.
    return is_missing_or_dirty | is_not_ten_digits | is_invalid_start | is_reserved_prefix_3


def is_invalid_identifiers(email_stripped, phone_int, birth_date_dt):
    """
    Polars expression: TRUE if any required identifier is invalid/missing/unparsable.
    (i.e., this returns the ANOMALY flag).
    """
    # Check 1: Birthdate is invalid if cast failed
    is_bd_invalid = birth_date_dt.is_null()

    # Check 2: Email is invalid if NULL/empty OR if it fails the regex match
    is_email_invalid = (
        email_stripped.is_null() |
        (email_stripped.str.len_chars() == 0) |
        # Use str.contains() with the regex. The negation (~) means failure to match is an ANOMALY.
        ~email_stripped.str.contains(EMAIL_REGEX, literal=False)
    )

    # Check 3: Phone is invalid if cast failed
    is_phone_invalid = is_invalid_phone_math(phone_int)

    return is_bd_invalid | is_email_invalid | is_phone_invalid


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
        col("birth_date").str.strptime(pl.Date, format="%m/%d/%Y", strict=False).alias("birth_date_dt"),
        col("created_at").str.strptime(pl.Datetime, format="%m/%d/%Y", strict=False).alias("created_at_dt"),

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
    return flagged_df

