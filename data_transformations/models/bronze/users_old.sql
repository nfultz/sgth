-- data_transformation/models/bronze/raw_data.sql
-- Simply select all columns from the dbt seed table

SELECT
    *
FROM {{ ref('challenge_dataset') }}
