-- models/staging/servicenow_tickets_date_extracted.sql

SELECT
    *,
    EXTRACT(YEAR FROM created_date) AS created_year,
    EXTRACT(MONTH FROM created_date) AS created_month,
    EXTRACT(DAY FROM created_date) AS created_day
FROM {{ ref('servicenow_tickets_cleaned') }}
