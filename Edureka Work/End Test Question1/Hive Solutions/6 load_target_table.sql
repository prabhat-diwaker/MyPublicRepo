
use edureka_dw;
insert overwrite table employee_details_latest
SELECT
         e.employee_id, 
         e.job_id, 
         e.manager_id, 
         e.department_id,
         d.location_id,
         l.country_id,
         e.first_name,
         e.last_name,
         e.salary,
         e.commission_pct,
         d.department_name,
         j.job_title,
         l.city,
         l.state_province,
         c.country_name,
         r.region_name
   FROM
      employees_scd e,
      departments_table d,
      jobs_table j,
      locations_table l,
      countries_table c,
      regions_table r
   WHERE e.department_id = d.department_id
   AND d.location_id = l.location_id
   AND l.country_id = c.country_id
   AND c.region_id = r.region_id
   AND j.job_id = e.job_id 
   and e.active_flag = 1;