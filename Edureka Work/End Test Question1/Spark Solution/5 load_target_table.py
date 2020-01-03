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

    employee_scd = spark.read.table("edureka_dw.employees_scd").filter(func.col("active_flag")==1)
    jobs = spark.read.table("edureka_dw.jobs_table")
    regions = spark.read.table("edureka_dw.regions_table")
    departments = spark.read.table("edureka_dw.departments_table")
    locations = spark.read.table("edureka_dw.locations_table")
    countries = spark.read.table("edureka_dw.countries_table")


    #df_emp_delta.show()
    latest_details = employee_scd.alias("scd")\
                                .join(departments.alias("departments"),employee_scd.department_id==departments.department_id,"inner")\
    .join(locations.alias("locations"), departments.location_id == locations.location_id, "inner")\
    .join(countries.alias("countries"), locations.country_id == countries.country_id, "inner")\
    .join(regions.alias("regions"), countries.region_id == regions.region_id, "inner")\
    .join(jobs.alias("jobs"), employee_scd.job_id == jobs.job_id, "inner")\
    .select("scd.employee_id",
"scd.job_id",
"scd.manager_id",
"scd.department_id",
"locations.location_id",
"countries.country_id",
"first_name",
"last_name",
"salary",
"commission_pct",
"department_name",
"job_title",
"locations.city",
"locations.state_province",
"countries.country_name",
"regions.region_name")

    latest_details.write.format("orc").mode("overwrite").saveAsTable("edureka_dw.employee_details_latest")
