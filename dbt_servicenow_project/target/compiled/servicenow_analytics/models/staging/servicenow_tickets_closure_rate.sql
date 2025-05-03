-- models/staging/servicenow_tickets_closure_rate.sql

SELECT
    assigned_group,
    COUNT(*) AS total_tickets,
    SUM(CASE WHEN status = 'Closed' THEN 1 ELSE 0 END) AS closed_tickets,
    ROUND(
        SUM(CASE WHEN status = 'Closed' THEN 1 ELSE 0 END)::DECIMAL
        / NULLIF(COUNT(*), 0), 2
    ) AS closure_rate
FROM "db"."analytics"."servicenow_tickets_cleaned"
GROUP BY assigned_group