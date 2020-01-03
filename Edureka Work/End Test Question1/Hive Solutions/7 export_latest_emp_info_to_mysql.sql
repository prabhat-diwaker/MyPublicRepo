CREATE table labuser_database.employee_details_latest
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

  


sqoop-export -Dmapreduce.job.running.map.limit=30 -Dsqoop.export.records.per.statement=5000 -Dsqoop.export.statements.per.transaction=100000 --num-mappers 2 --connect "jdbc:mysql://dbserver.edu.cloudlab.com/labuser_database" --table employee_details_latest  --update-mode allowinsert --hcatalog-table employee_details_latest --hcatalog-database edureka_dw --username edu_labuser --password edureka  
