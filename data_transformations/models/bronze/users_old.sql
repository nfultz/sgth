-- data_transformations/models/bronze/users_old.sql
-- Simply select all columns from the dbt seed table

SELECT
    *
FROM {{ ref('challenge_dataset') }}
