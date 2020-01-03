--- Identify new records in source which is not present in SCD .

drop table if exists edureka_dw.new_employees_in_source;
create temporary table edureka_dw.new_employees_in_source stored as orc as 
SELECT 
    src.*,
    '1900-01-01' start_date,
    '9999-12-31' end_date,
    '1' Active_Flag,
    current_timestamp() row_insertion_dttm
FROM edureka_dw.employees_table src
left outer join 
edureka_dw.employees_scd scd 
on src.employee_id = scd.employee_id
where scd.employee_id is null;



--- Identify all the updated employee codes in source wrt SCD table if any of the SCD columns has changed

drop table if exists edureka_dw.updated_employees_ids_in_source;
create temporary table edureka_dw.updated_employees_ids_in_source stored as orc as 
SELECT 
    DISTINCT
    src.employee_id
FROM edureka_dw.employees_table src
INNER JOIN 
(
    select * from edureka_dw.employees_scd where Active_Flag='1' and end_date ='9999-12-31') scd
on (src.employee_id = scd.employee_id)
where
(src.email <> scd.email or 
src.phone_number <> scd.phone_number or
src.job_id <> scd.job_id or
src.commission_pct <> scd.commission_pct or
src.salary <> scd.salary or
src.manager_id <> scd.manager_id or
src.department_id <> scd.department_id 
);


-- For all the above employee codes which have updates in source, Make existing SCD record inactive and close the active recods with current date

drop table if exists edureka_dw.updated_employees_in_source;
create temporary table edureka_dw.updated_employees_in_source stored as orc as 
select 
    scd.employee_id,
    first_name,
    last_name,
    email,
    phone_number,
    hire_date,
    job_id,
    salary,
    commission_pct,
    manager_id,
    department_id,
    start_date,
    case when end_date ='9999-12-31' then date_sub(to_date(current_timestamp()),1) else end_date end as end_date,
    '0' Active_Flag,  
    current_timestamp() row_insertion_dttm
FROM edureka_dw.employees_scd scd
inner join edureka_dw.updated_employees_ids_in_source codes on scd.employee_id=codes.employee_id 

union all

SELECT 
    src.employee_id,
    first_name,
    last_name,
    email,
    phone_number,
    hire_date,
    job_id,
    salary,
    commission_pct,
    manager_id,
    department_id,
    to_date(current_timestamp()) start_date,
    '9999-12-31' end_date,
    '1' Active_Flag,
    current_timestamp() row_insertion_dttm
FROM edureka_dw.employees_table src
inner join
edureka_dw.updated_employees_ids_in_source codes on src.employee_id=codes.employee_id ;

drop table if exists edureka_dw.unchanged_employees_in_scd;
create temporary table edureka_dw.unchanged_employees_in_scd stored as orc as 
select 
    scd.employee_id,
    first_name,
    last_name,
    email,
    phone_number,
    hire_date,
    job_id,
    salary,
    commission_pct,
    manager_id,
    department_id,
start_date,
end_date,
Active_Flag,
scd.row_insertion_dttm
FROM edureka_dw.employees_scd scd
left join edureka_dw.updated_employees_ids_in_source codes on scd.employee_id=codes.employee_id 
where codes.employee_id is null;



----- overwrite final SCD table 

insert overwrite table edureka_dw.employees_scd 
select distinct * from edureka_dw.unchanged_employees_in_scd
union all 
select distinct * from edureka_dw.updated_employees_in_source
union all
select distinct * from edureka_dw.new_employees_in_source
;
