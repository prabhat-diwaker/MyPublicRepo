#--- problem statement
#--- We have dim_item table in labuser_database db in mysql. We need to import this as a parquet file to a HDFS location and load this data to a partitioned ORC target hive table

#--- Sqoop command to import data from mysql dim_item table to HDFS location as parquet file

"""sqoop import --connect "jdbc:mysql://dbserver.edu.cloudlab.com/labuser_database"  \
--username edu_labuser \
--password  "edureka" \
--query "select * from dim_item where \$CONDITIONS" \
--as-parquetfile \
--target-dir /tmp/edureka_719925/dim_item \
--m 1 \
--delete-target-dir"""

#above sqoop command will import data from mysql to hdfs location '/tmp/edureka_719925/dim_item' in parquet format. we will use this location to read
#data in spark

from pyspark.sql import SparkSession
from pyspark.sql.functions import expr


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

    spark.sql("set spark.sql.hive.convertMetastoreOrc=true")
    spark.sql("set hive.exec.dynamic.partition=true")
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    df = spark.read.parquet("/tmp/edureka_719925/dim_item/*.parquet")
    df_stage = df.select(
                        'mds_fam_id',
                        'geo_region_cd',
                        'curr_ind',
                        'item_nbr',
                        'item_desc_1',
                        'upc_nbr',
                        'cntry_nm',
                        'dept_nbr',
                        'dept_desc',
                        'op_cmpny_cd',
                        'dept_catg_nbr',
                        'dept_catg_desc',
                        'prod_nbr',
                        'prod_desc',
                        'brand_id',
                        'brand_nm',
                        'brand_ownr_nm',
                        'item_type_desc',
                        expr("current_timestamp() as row_insertion_dttm")
                        )

    q= "drop table if exists edureka_dw.dim_item"
    spark.sql(q)


    dim_item_query = """CREATE TABLE edureka_dw.dim_item (
                                                    `mds_fam_id` int, 
                                                      `curr_ind` tinyint, 
                                                      `item_nbr` int, 
                                                      `item_desc_1` string, 
                                                      `upc_nbr` decimal(18,0), 
                                                      `cntry_nm` string, 
                                                      `dept_nbr` smallint, 
                                                      `dept_desc` string, 
                                                      `op_cmpny_cd` string, 
                                                      `dept_catg_nbr` int, 
                                                      `dept_catg_desc` string, 
                                                      `prod_nbr` int, 
                                                      `prod_desc` string, 
                                                      `brand_id` int, 
                                                      `brand_nm` string, 
                                                      `brand_ownr_nm` string, 
                                                      `item_type_desc` string,
                                                      row_insertion_dttm string
                                                      )
                                                      partitioned by 
                                                      (
                                                      geo_region_cd char(2)
                                                      )
                                                    STORED AS ORC
                                                    """

    spark.sql(dim_item_query)

    df_stage.write.mode("overwrite").format("orc").partitionBy('geo_region_cd').saveAsTable('edureka_dw.dim_item')