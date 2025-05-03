{{
  config(
    materialized='table'
  )
}}

-- Simple ticket count by category
SELECT
   inc_category, COUNT(*) AS ticket_count
FROM 
    {{ source('servicenow', 'servicenow_tickets_raw') }}
GROUP BY 
    inc_category
ORDER BY 
    ticket_count DESC
