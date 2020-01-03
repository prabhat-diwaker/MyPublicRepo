from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
import pyspark.sql.functions as func
from pyspark.sql.functions import broadcast
def init():
    spark = SparkSession \
        .builder \
        .master("yarn") \
        .appName("dim_item_load") \
        .config("spark.dynamicAllocation.enabled", "true") \
        .config("spark.shuffle.service.enabled", "true") \
        .config("spark.dynamicAllocation.initialExecutors", 1) \
        .config("spark.dynamicAllocation.minExecutors", 1) \
        .config("spark.dynamicAllocation.maxExecutors", 30) \
        .config('spark.executor.cores', 1) \
        .config("spark.network.timeout", '180s') \
        .enableHiveSupport() \
        .getOrCreate()
    return spark
if __name__ == '__main__':
    spark = init()
    #spark.sql("set spark.sql.hive.convertMetastoreOrc=true")
    spark.sql("set hive.exec.dynamic.partition=true")
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    spark.sql("set hive.auto.convert.join=true")
    df_emp_delta = spark.read.csv("/tmp/edureka_719925/ECT1_employee_delta/employee_delta.txt",header="true",inferSchema="true",sep="|")
    df_emp_scd = spark.read.table("edureka_dw.employees_scd").withColumnRenamed("employee_id","employee_id_scd")
    #df_emp_delta.show()
    new_employees_in_source = df_emp_delta.alias("base")\
                                .join(df_emp_scd.alias("scd"),(df_emp_scd.employee_id_scd==df_emp_delta.employee_id),"left")\
                                .where(func.isnull(func.col("employee_id_scd"))) \
                                .select("base.employee_id",
                                        "base.first_name",
                                        "base.last_name",
                                        "base.email",
                                        "base.phone_number",
                                        "base.hire_date",
                                        "base.job_id",
                                        "base.salary",
                                        "base.commission_pct",
                                        "base.manager_id",
                                        "base.department_id",
                                        expr("'1900-01-01' as start_date"),
                                        expr("'9999-12-31' as end_date"),
                                        expr("'1' as active_flag"),
                                        expr("current_timestamp() as row_insertion_dttm")
                                        )

    #df_new_employees.show()

    active_employee_records = df_emp_scd.filter(func.col("active_flag")==1)
    #active_employee_records.show()
    updated_employees_ids_in_source = df_emp_delta.alias("base")\
                        .join(active_employee_records.alias("active"),df_emp_delta.employee_id==active_employee_records.employee_id_scd,"inner")\
                        .filter((func.col("base.email") <> func.col("active.email"))
                                |(func.col("base.phone_number") <> func.col("active.phone_number"))
                                | (func.col("base.job_id") <> func.col("active.job_id"))
                                | (func.col("base.commission_pct") <> func.col("active.commission_pct"))
                                | (func.col("base.salary") <> func.col("active.salary"))
                                | (func.col("base.manager_id") <> func.col("active.manager_id"))
                                | (func.col("base.department_id") <> func.col("active.department_id"))
                                )\
                        .select("base.employee_id")\
                        .dropDuplicates(['employee_id'])

    #updated_employees_ids_in_source.show()

    updated_employees_in_source_stg1 = df_emp_scd.alias("Scd")\
                                                .join(updated_employees_ids_in_source.alias("updated"),df_emp_scd.employee_id_scd==updated_employees_ids_in_source.employee_id,"inner")\
                                                .select("employee_id_scd",
                                                        "first_name",
                                                        "last_name",
                                                        "email",
                                                        "phone_number",
                                                        "hire_date",
                                                        "job_id",
                                                        "salary",
                                                        "commission_pct",
                                                        "manager_id",
                                                        "department_id",
                                                        "start_date",
                                                        expr("case when end_date ='9999-12-31' then date_sub(to_date(current_timestamp()),1) else end_date end as end_date"),
                                                        expr("'0' Active_Flag"),
                                                        expr("current_timestamp() as row_insertion_dttm")
                                                        )
    #updated_employees_in_source_stg1.show()

    updated_employees_in_source_stg2 = df_emp_delta.alias("base")\
                                                .join(updated_employees_ids_in_source.alias("updated"),df_emp_delta.employee_id==updated_employees_ids_in_source.employee_id,"inner")\
                                                .select("base.employee_id",
                                                        "first_name",
                                                        "last_name",
                                                        "email",
                                                        "phone_number",
                                                        "hire_date",
                                                        "job_id",
                                                        "salary",
                                                        "commission_pct",
                                                        "manager_id",
                                                        "department_id",
                                                        expr("to_date(current_timestamp()) as start_date"),
                                                        expr("'9999-12-31' as end_date"),
                                                        expr("'1' as Active_Flag"),
                                                        expr("current_timestamp() as row_insertion_dttm")
                                                        )

    #updated_employees_in_source_stg2.show()

    updated_employees_in_source = updated_employees_in_source_stg1.unionAll(updated_employees_in_source_stg2)

    unchanged_employees_in_scd = df_emp_scd.alias("scd")\
                                            .join(updated_employees_ids_in_source.alias("updated"),updated_employees_ids_in_source.employee_id==df_emp_scd.employee_id_scd,"left") \
                                            .where(func.isnull(func.col("employee_id"))) \
                                            .select(    "employee_id_scd",
                                                        "first_name",
                                                       "last_name",
                                                       "email",
                                                       "phone_number",
                                                       "hire_date",
                                                       "job_id",
                                                       "salary",
                                                       "commission_pct",
                                                       "manager_id",
                                                       "department_id",
                                                        "start_date",
                                                        "end_date",
                                                        "Active_Flag",
                                                        "row_insertion_dttm"
                                                    )

    employee_scd_final = unchanged_employees_in_scd\
                                        .unionAll(updated_employees_in_source)\
                                        .unionAll(new_employees_in_source)



    employee_scd_final.write.mode("overwrite").saveAsTable("edureka_dw.scd_temp")

    temp_df = spark.read.table("edureka_dw.scd_temp").withColumnRenamed("employee_id_scd","employee_id")
    temp_df.write.format("orc").mode("overwrite").saveAsTable("edureka_dw.employees_scd")
    