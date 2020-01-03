------------------------------------------countries_table---------------------------------------
drop table if exists edureka_dw.countries_table;
CREATE TABLE edureka_dw.countries_table
   ( country_id CHAR(2) 
   , country_name VARCHAR(40) 
   , region_id int 
   ) 
   row format delimited
   fields terminated by '|';


------------------------------------------departments_table---------------------------------------
drop table if exists edureka_dw.departments_table;
CREATE TABLE edureka_dw.departments_table
   ( department_id int
   , department_name VARCHAR(30)
   , manager_id int
   , location_id int
   ) 
   row format delimited
   fields terminated by '|';

   

------------------------------------------employees_table---------------------------------------
drop table if exists edureka_dw.employees_table;
CREATE TABLE edureka_dw.employees_table
   ( employee_id int
   , first_name VARCHAR(20)
   , last_name VARCHAR(25)
   , email VARCHAR(25)
   , phone_number VARCHAR(20)
   , hire_date varchar(12)
   , job_id VARCHAR(10)
   , salary decimal
   , commission_pct decimal(2,2)
   , manager_id int
   , department_id int
   ) 
   row format delimited
   fields terminated by '|';



------------------------------------------jobs_table---------------------------------------
drop table if exists edureka_dw.jobs_table;
CREATE TABLE edureka_dw.jobs_table
   ( job_id VARCHAR(10)
   , job_title VARCHAR(35)
   , min_salary int
   , max_salary int
   
) 
   row format delimited
   fields terminated by '|';


------------------------------------------locations_table---------------------------------------
drop table if exists edureka_dw.locations_table;
CREATE TABLE edureka_dw.locations_table
   ( location_id int
   , street_address VARCHAR(40)
   , postal_code VARCHAR(12)
   , city VARCHAR(30)
   , state_province VARCHAR(25)
   , country_id CHAR(2)
   ) 
   row format delimited
   fields terminated by '|';



------------------------------------------regions_table---------------------------------------
drop table if exists edureka_dw.regions_table;
CREATE TABLE edureka_dw.regions_table
   ( region_id int 
   , region_name VARCHAR(25) 
   )
   row format delimited
   fields terminated by '|';

   


------------------------------------------employees_scd---------------------------------------
drop table if exists edureka_dw.employees_scd;
CREATE TABLE edureka_dw.employees_scd
   ( employee_id int
   , first_name VARCHAR(20)
   , last_name VARCHAR(25)
   , email VARCHAR(25)
   , phone_number VARCHAR(20)
   , hire_date varchar(12)
   , job_id VARCHAR(10)
   , salary decimal
   , commission_pct decimal(2,2)
   , manager_id int
   , department_id int
   , start_date string
   , end_date string
   , active_flag char(1)
   , row_insertion_dttm string
   ) ;

------------------------------------------employee_details_latest---------------------------------------

drop table if exists edureka_dw.employee_details_latest;
CREATE table edureka_dw.employee_details_latest
   (employee_id int,
   job_id varchar(10),
   manager_id int,
   department_id int,
   location_id int,
   country_id char(2),
   first_name varchar(20),
   last_name varchar(25),
   salary decimal,
   commission_pct decimal(2,2),
   department_name varchar(30),
   job_title varchar(35),
   city varchar(30),
   state_province varchar(25),
   country_name varchar(40), 
   region_name varchar(25)
   );


