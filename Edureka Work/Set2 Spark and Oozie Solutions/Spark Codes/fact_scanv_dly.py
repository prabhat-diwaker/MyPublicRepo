from pyspark.sql import SparkSession
from pyspark.sql.functions import expr


def init():
    spark = SparkSession \
        .builder \
        .master("yarn") \
        .appName("fact_scanv_load") \
        .config("spark.dynamicAllocation.enabled", "true") \
        .config("spark.shuffle.service.enabled", "true") \
        .config("spark.dynamicAllocation.initialExecutors", 10) \
        .config("spark.dynamicAllocation.minExecutors", 10) \
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

    df = spark.read.orc("/user/edureka_719925/scanv_data/*_*")
    df.registerTempTable("fact_scanv_dly_stg")

    q = "drop table if exists edureka_dw.fact_scanv_dly_stage"
    spark.sql(q)

    stg_q = """CREATE TABLE edureka_dw.fact_scanv_dly_stage (
                                                                `store_nbr` int, 
                                                                 `geo_region_cd` char(2), 
                                                                 `scan_id` int, 
                                                                 `scan_type` tinyint, 
                                                                 `dept_nbr` smallint, 
                                                                 `mds_fam_id` int, 
                                                                 `upc_nbr` decimal(18,0), 
                                                                 `sales_unit_qty` decimal(9,2), 
                                                                 `sales_amt` decimal(9,2), 
                                                                 `cost_amt` decimal(9,2), 
                                                                 `visit_dt` date, 
                                                                 `rtn_amt` decimal(9,2), 
                                                                 `crncy_cd` char(3), 
                                                                 `op_cmpny_cd` varchar(10))
                                                               ROW FORMAT SERDE 
                                                                 'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
                                                               STORED AS INPUTFORMAT 
                                                                 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
                                                               OUTPUTFORMAT 
                                                                 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
                                                               TBLPROPERTIES (
                                                                 'COLUMN_STATS_ACCURATE'='true'
                                                                              )
                                                               """
    spark.sql(stg_q)

    stage_load_q  = "insert overwrite table edureka_dw.fact_scanv_dly_stage select * from fact_scanv_dly_stg"
    spark.sql(stage_load_q)

    q= "drop table if exists edureka_dw.fact_scanv_dly"
    spark.sql(q)


    fact_scanv_dly_query = """CREATE TABLE edureka_dw.fact_scanv_dly (
                                                    `store_nbr` int, 
                                                     `geo_region_cd` char(2), 
                                                     `scan_id` int, 
                                                     `scan_type` tinyint, 
                                                     `dept_nbr` smallint, 
                                                     `mds_fam_id` int, 
                                                     `upc_nbr` decimal(18,0), 
                                                     `sales_unit_qty` decimal(9,2), 
                                                     `sales_amt` decimal(9,2), 
                                                     `cost_amt` decimal(9,2), 
                                                     `rtn_amt` decimal(9,2), 
                                                     `crncy_cd` char(3),
                                                     row_insertion_dttm string
                                                     )
                                                   partitioned by   
                                                   (
                                                   `visit_dt` date,                                               
                                                   `op_cmpny_cd` varchar(8)
                                                    )
                                                   ROW FORMAT SERDE 
                                                     'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
                                                   STORED AS INPUTFORMAT 
                                                     'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
                                                   OUTPUTFORMAT 
                                                     'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
                                                   TBLPROPERTIES (
                                                     'COLUMN_STATS_ACCURATE'='true'
                                                                  )
                                                    """

    spark.sql(fact_scanv_dly_query)

    load_q = """insert overwrite table edureka_dw.fact_scanv_dly partition (visit_dt,op_cmpny_cd) 
                select 
                    store_nbr ,
                    geo_region_cd , 
                    scan_id  ,
                    scan_type ,
                    dept_nbr ,
                    mds_fam_id ,
                    upc_nbr ,
                    sales_unit_qty ,
                    sales_amt ,
                    cost_amt ,
                    rtn_amt ,
                    crncy_cd ,
                    current_timestamp() as row_insertion_dttm,
                    visit_dt  ,
                    op_cmpny_cd
                from edureka_dw.fact_scanv_dly_stage"""

    spark.sql(load_q)