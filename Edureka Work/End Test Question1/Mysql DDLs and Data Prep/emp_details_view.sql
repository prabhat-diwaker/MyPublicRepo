--https://download.oracle.com/oll/tutorials/DBXETutorial/html/module2/les02_load_data_sql.htm

https://docs.oracle.com/cd/B28359_01/server.111/b28328.pdf

CREATE OR REPLACE VIEW emp_details_view
   (employee_id,
   job_id,
   manager_id,
   department_id,
   location_id,
   country_id,
   first_name,
   last_name,
   salary,
   commission_pct,
   department_name,
   job_title,
   city,
   state_province,
   country_name,
   region_name)
   AS SELECT
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
   employees_base_table e,
   departments_table d,
   jobs j,
   locations l,
   countries_table c,
   regions r
   WHERE e.department_id = d.department_id
   AND d.location_id = l.location_id
   AND l.country_id = c.country_id
   AND c.region_id = r.region_id
   AND j.job_id = e.job_id 