drop table if exists edureka_dw.employees_delta;
CREATE TABLE edureka_dw.employees_delta
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

load data local inpath '/mnt/bigdatapgp/edureka_719925/endTest1/emp_delta/employee_delta.txt' into table edureka_dw.employees_delta;