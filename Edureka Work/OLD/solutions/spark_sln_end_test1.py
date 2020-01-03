# pre-requisite : All the required tables are already loaded in Hive

from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
import pyspark.sql.functions as func
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number


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

    dim_item_df = spark.read.table("edureka_dw.dim_item")
    dim_store_df = spark.read.table("edureka_dw.dim_store")
    fact_sales_df = spark.read.table("edureka_dw.fact_sales")
    sales_amount_df = spark.read.table("edureka_dw.sales_amount")


    """select 
		a.*,
		b.item_desc_1,
		b.dept_catg_desc,
		c.store_nm 
	from fact_sales a 
	inner join dim_item b on a.item_id=b.mds_fam_id
	inner join dim_store c on a.store_nbr= c.store_nbr"""



    stg_df = fact_sales_df.join(dim_item_df,(fact_sales_df.item_id==dim_item_df.mds_fam_id),"inner")\
                          .join(dim_store_df,(fact_sales_df.store_nbr==dim_store_df.store_nbr),"inner")\
                          .drop(dim_store_df.store_nbr)\
                          .select\
                            (
                                "item_id",
                                "store_nbr",
                                "sales",
                                "item_desc_1",
                                "dept_catg_desc",
                                "store_nm"
                            )

    stg_df.registerTempTable("stage")

    store_item_category_sales_query = \
                            """ select 
	                                store_nbr,
	                                store_nm,
	                                dept_catg_desc,
	                                sales_ids 
	                            from stage
	                            lateral view explode(sales) temp_table as sales_ids
	                        """
    top_selling_category_stores_df = spark.sql(store_item_category_sales_query)


    stg_df = top_selling_category_stores_df.join(sales_amount_df,(top_selling_category_stores_df.sales_ids == sales_amount_df.sales_id),"inner")\
                                            .select\
                                            (
                                                "store_nbr",
                                                "store_nm",
                                                "dept_catg_desc",
                                                expr("to_date(transaction_date) as sales_date"),
                                                "sales_amount"
                                             ).groupBy(
        "store_nbr",
        "store_nm",
        "dept_catg_desc",
        "sales_date"
    )\
    .agg({"sales_amount":"sum"}).select\
    ("store_nbr",
     "store_nm",
     "dept_catg_desc",
     "sales_date",
     func.col("sum(sales_amount)").alias("total_sales")
     )


    windowSpec = Window.partitionBy("store_nbr", "store_nm", "sales_date").orderBy(func.col("total_sales").desc())

    final_df = stg_df.withColumn("rnk",row_number().over(windowSpec)).select(
        "store_nbr",
        "store_nm",
        "dept_catg_desc",
        "total_sales",
        "sales_date",
        "rnk"
    ).filter(func.col("rnk")==1)


    final_df.coalesce(1).write.format("orc").mode("overwrite").saveAsTable("edureka_dw.top_selling_category_stores")