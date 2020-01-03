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

    df_fact_instk = spark.read.table("edureka_dw.fact_instk_dly").filter(func.col("op_cmpny_cd")=="WMT-US")
    df_dim_item = spark.read.table("edureka_dw.dim_item").filter(func.col("geo_region_cd")=="US")
    df_dim_store = spark.read.table("edureka_dw.dim_store").filter(func.col("op_cmpny_cd")=="WMT-US")
    df_dim_calendar = spark.read.table("edureka_dw.dim_calendar").filter(func.col("geo_region_cd")=="US")
    df_dim_region = spark.read.table("edureka_dw.dim_region").filter(func.col("op_cmpny_cd")=="WMT-US")




    instk_with_dim_df  =  df_fact_instk.join(broadcast(df_dim_store),(df_dim_store.store_nbr==df_fact_instk.store_nbr),"inner")\
                                                .drop(df_dim_store.store_nbr) \
                                                .drop(df_dim_store.op_cmpny_cd) \
                                                .drop(df_dim_store.geo_region_cd) \
                                                .drop(df_dim_store.region_nm) \
                                        .join(broadcast(df_dim_item),(df_dim_item.mds_fam_id==df_fact_instk.mds_fam_id),"inner")\
                                                .drop(df_dim_item.mds_fam_id) \
                                                .drop(df_dim_item.geo_region_cd) \
                                                .drop(df_dim_item.op_cmpny_cd) \
                                        .join(broadcast(df_dim_calendar),(df_dim_calendar.cal_dt==df_fact_instk.bus_dt),"inner") \
                                                .drop(df_dim_calendar.geo_region_cd) \
                                        .join(broadcast(df_dim_region),(df_dim_store.region_nbr==df_dim_region.region_nbr),"inner")\
                                                .drop(df_dim_region.op_cmpny_cd) \
                                                .drop(df_dim_region.geo_region_cd) \
                                        .select(
                                                	'mds_fam_id',
                                                    'store_nbr',
                                                    'onhand_each_qty',
                                                    'fcst_dmand_each_qty',
                                                    "op_cmpny_cd",
                                                    "bus_dt",
                                                    "store_nm",
                                                    "market_nm",
                                                    "city_nm",
                                                    "item_desc_1",
                                                    "dept_desc",
                                                    "brand_nm",
                                                    "prod_desc",
                                                    "wm_yr_wk_nbr",
                                                    "wm_qtr_nm",
                                                    expr('substring(cal_dt,1,3) as year'),
                                                    "region_nm",
                                                    "buo_area_nm"
                                                )

    store_scanv_dly_df = spark.read.table("edureka_dw.fact_scanv_dly").filter(func.col("op_cmpny_cd") == "WMT-US")\
                                            .groupBy("store_nbr","mds_fam_id","visit_dt")\
                                            .agg({"sales_unit_qty": "sum", "sales_amt": "sum","rtn_amt": "sum"}) \
                                            .select("store_nbr",
                                                    "mds_fam_id",
                                                    "visit_dt",
                                                    func.col("sum(sales_unit_qty)").alias("sales_unit_qty"),
                                                    func.col("sum(sales_amt)").alias("sales_amt"),
                                                    func.col("sum(rtn_amt)").alias("rtn_amt")
                                                    )


    fact_stores_out_of_stock_df = instk_with_dim_df.join(store_scanv_dly_df, (
                store_scanv_dly_df.mds_fam_id == instk_with_dim_df.mds_fam_id) &
                                                         (store_scanv_dly_df.store_nbr == instk_with_dim_df.store_nbr) &
                                                         (store_scanv_dly_df.visit_dt == instk_with_dim_df.bus_dt)
                                                         , "inner"
                                                         ).drop(store_scanv_dly_df.mds_fam_id) \
                                                            .drop(store_scanv_dly_df.store_nbr) \
                                                            .select(
                                                                     "mds_fam_id",
                                                                "store_nbr",
                                                                "onhand_each_qty",
                                                                "fcst_dmand_each_qty",
                                                                "store_nm",
                                                                "market_nm",
                                                                "city_nm",
                                                                "item_desc_1",
                                                                "dept_desc",
                                                                "brand_nm",
                                                                "prod_desc",
                                                                "wm_yr_wk_nbr",
                                                                "wm_qtr_nm",
                                                                "year",
                                                                "region_nm",
                                                                "buo_area_nm",
                                                                "sales_unit_qty",
                                                                "sales_amt",
                                                                "rtn_amt",
                                                                "op_cmpny_cd",
                                                                 "bus_dt"
                                                                )

    fact_stores_out_of_stock_df.coalesce(10).write.mode("overwrite").format("orc").partitionBy('op_cmpny_cd', 'bus_dt').saveAsTable('edureka_dw.fact_stores_out_of_stock')