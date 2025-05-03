-- models/staging/servicenow_tickets_avg_resolution_time.sql

SELECT
    category,
    priority,
    AVG(EXTRACT(EPOCH FROM (resolved_date - created_date)) / 3600) AS avg_resolution_time_hrs
FROM {{ ref('servicenow_tickets_cleaned') }}
GROUP BY category, priority
