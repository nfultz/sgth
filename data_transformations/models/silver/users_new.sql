-- This model applies the DDL contract and materializes the final table.
-- Must be a table to enforce physical constraints
{{ config(
    materialized = 'table'
) }}

-- Select all columns from the temporary, remediated Python view
SELECT *
FROM {{ ref('users_stage') }}
