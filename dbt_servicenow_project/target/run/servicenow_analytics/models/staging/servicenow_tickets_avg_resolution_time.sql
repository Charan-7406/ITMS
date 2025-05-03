
  
    

  create  table "db"."analytics"."servicenow_tickets_avg_resolution_time__dbt_tmp"
  
  
    as
  
  (
    -- models/staging/servicenow_tickets_avg_resolution_time.sql

SELECT
    category,
    priority,
    AVG(EXTRACT(EPOCH FROM (resolved_date - created_date)) / 3600) AS avg_resolution_time_hrs
FROM "db"."analytics"."servicenow_tickets_cleaned"
GROUP BY category, priority
  );
  