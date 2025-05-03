
  
    

  create  table "db"."analytics"."check__dbt_tmp"
  
  
    as
  
  (
    

-- Simple ticket count by category
SELECT
   inc_category, COUNT(*) AS ticket_count
FROM 
    "db"."public"."servicenow_tickets_raw"
GROUP BY 
    inc_category
ORDER BY 
    ticket_count DESC
  );
  