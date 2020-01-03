# --problem statement :
# -- region data is placed at hdfs path as json file. We need to create a managed table and load the data from this file



from pyspark.sql import SparkSession
from pyspark.sql.functions import expr


def init():
    spark = SparkSession \
        .builder \
        .master("yarn") \
        .appName("dim_region_load") \
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

    df = spark.read.json("/user/edureka_719925/region_data/region.json").select(
        'subdiv.subdiv_mgr_nm',
        'subdiv.subdiv_nm',
        'subdiv.subdiv_nbr',
        'region.region_nm',
        'region.region_mgr_nm',
        'region.region_nbr',
        'recorddetails.upd_userid',
        'recorddetails.load_userid',
        'recorddetails.load_ts',
        'recorddetails.upd_ts',
        'geo_region_cd',
        'buo.buo_area_mgr_nm',
        'buo.buo_area_nbr',
        'buo.buo_area_nm',
        'recorddetails.op_cmpny_cd',
        expr("current_timestamp() as row_insertion_dttm")
    )

    q = "drop table if exists edureka_dw.dim_region"
    spark.sql(q)

    dim_region_q = """create table edureka_dw.dim_region
                                (
                                    subdiv_mgr_nm string,
                                    subdiv_nm string,
                                    subdiv_nbr int,
                                    region_nm string,
                                    region_mgr_nm string,
                                    region_nbr int,
                                    upd_userid string,
                                    load_userid string,
                                    load_ts string,
                                    upd_ts string,
                                    geo_region_cd string,
                                    buo_area_mgr_nm string,
                                    buo_area_nbr int,
                                    buo_area_nm string,
                                    row_insertion_dttm string
                                )
                                partitioned by
                                (
                                    op_cmpny_cd string
                                )
                                stored as orc """

    spark.sql(dim_region_q)

    df.write.mode("overwrite").format("orc").partitionBy('op_cmpny_cd').saveAsTable('edureka_dw.dim_region')