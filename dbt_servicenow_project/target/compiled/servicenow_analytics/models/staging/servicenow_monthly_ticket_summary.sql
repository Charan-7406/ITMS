-- models/staging/servicenow_monthly_ticket_summary.sql

SELECT
    DATE_TRUNC('month', created_date) AS month,
    category,
    priority,
    COUNT(*) AS total_tickets,
    ROUND(AVG(EXTRACT(EPOCH FROM (resolved_date - created_date)) / 3600), 2) AS avg_resolution_time_hrs,
    ROUND(
        SUM(CASE WHEN status = 'Closed' THEN 1 ELSE 0 END)::DECIMAL
        / NULLIF(COUNT(*), 0), 2
    ) AS closure_rate
FROM "db"."analytics"."servicenow_tickets_cleaned"
GROUP BY 1, 2, 3
ORDER BY 1