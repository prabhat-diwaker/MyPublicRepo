# --problem statement :
# -- calendat data is placed at local edge node path as $ separated file. We need to create a manager table and load the data from this file



from pyspark.sql import SparkSession
from pyspark.sql.functions import expr


def init():
    spark = SparkSession \
        .builder \
        .master("yarn") \
        .appName("dim_calendar_load") \
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

    spark.sql("set spark.sql.hive.convertMetastoreOrc=true")
    spark.sql("set hive.exec.dynamic.partition=true")
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    df = spark.read.csv("/tmp/edureka_719925/*_0",sep="$",header="true",inferSchema="true").withColumn("cal_dt",expr("to_date(cal_dt) as cal_dt"))
    df_stage = df.select(
                        'cal_dt',
                        'date_desc',
                        'wm_wk_day_nbr',
                        'wm_yr_wk_nbr',
                        'wm_mth_nbr',
                        'wm_mth_nm',
                        'wm_qtr_nm',
                        'wm_half_yr_nbr',
                        'wm_half_yr_nm',
                        'wm_yr_desc',
                        'geo_region_cd',
                        expr("current_timestamp() as row_insertion_dttm")
                        )

    q= "drop table if exists edureka_dw.dim_calendar"
    spark.sql(q)


    dim_calendar_query = """CREATE TABLE edureka_dw.dim_calendar(
                                                            `cal_dt` date, 
                                                              `date_desc` varchar(50), 
                                                              `wm_wk_day_nbr` smallint, 
                                                              `wm_yr_wk_nbr` int, 
                                                              `wm_mth_nbr` int, 
                                                              `wm_mth_nm` varchar(50), 
                                                              `wm_qtr_nm` varchar(50), 
                                                              `wm_half_yr_nbr` int, 
                                                              `wm_half_yr_nm` varchar(50), 
                                                              `wm_yr_desc` varchar(50),
                                                              `row_insertion_dttm` string
                                                              )
                                                            partitioned by 
                                                            (
                                                             `geo_region_cd` string
                                                             )
                                                             STORED as ORC
                                                    """

    spark.sql(dim_calendar_query)

    df_stage.write.mode("overwrite").format("orc").partitionBy('geo_region_cd').saveAsTable('edureka_dw.dim_calendar')