

WITH cleaned AS (
    SELECT DISTINCT
        inc_number AS ticket_id,
        inc_category AS category,
        inc_priority AS priority,
        inc_sys_created_on AS created_date,
        inc_resolved_at AS resolved_date,
        inc_assignment_group AS assigned_group,
        inc_state AS status,
        inc_assigned_to AS technician,
        inc_close_code AS close_code,
        inc_close_notes AS close_notes
    FROM "db"."public"."servicenow_tickets_raw"
    WHERE inc_sys_created_on IS NOT NULL
        AND inc_number IS NOT NULL
)

SELECT * FROM cleaned