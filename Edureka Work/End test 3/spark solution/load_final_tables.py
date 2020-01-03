from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
import pyspark.sql.functions as func
from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank,col,row_number
from pyspark.sql.functions import desc

import sys
import codecs
sys.stdout = codecs.getwriter('utf8')(sys.stdout)
sys.stderr = codecs.getwriter('utf8')(sys.stderr)

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

    orders = spark.read.table("edureka_dw.orders")
    product = spark.read.table("edureka_dw.product").withColumnRenamed("unitprice","productunitprice")
    product_r = spark.read.table("edureka_dw.product").withColumnRenamed("id", "r_id").withColumnRenamed("productname","r_productname")
    supplier = spark.read.table("edureka_dw.supplier")

    stg_df = orders.alias("a").join(product.alias("b"), orders.productid == product.id, "inner") \
        .select("b.id",
                "b.productname",
                expr("substr(a.orderdate,1,3)as sale_month"),
                expr("a.Quantity*a.unitprice as sell_amount"),
                expr("a.Quantity*b.productunitprice as buy_price")
                ) \
        .groupBy('id', 'productname', 'sale_month') \
        .agg({"sell_amount": "sum", "buy_price": "sum"}) \
        .withColumnRenamed("sum(sell_amount)", "total_sell_amount") \
        .withColumnRenamed("sum(buy_price)", "total_buy_price") \
        .select(
        "id",
        'productname',
        'sale_month',
        expr("total_sell_amount-total_buy_price as total_profit"),
        "total_buy_price"
    ) \
        .withColumn("rnk", dense_rank().over(Window.partitionBy("sale_month").orderBy(desc("total_profit")))) \
        .filter((func.col("rnk") == 1) & (col("total_profit") > 0))

    stg_df_1 = stg_df.alias("stg").join(product.alias("p"), product.id == stg_df.id, "left") \
        .select("sale_month",
                "stg.productname",
                "total_profit",
                expr("round((total_profit/total_buy_price)*100,2) as profit_percent"),
                "p.supplierid"
                )

    final_df_profitable_product = stg_df_1.join(supplier.alias("s"), stg_df_1.supplierid == supplier.id, "left") \
        .select("sale_month",
                "productname",
                expr("companyname as suppliername"),
                "total_profit",
                "profit_percent"
                )

    final_df_profitable_product.write.format("orc").mode("overwrite").saveAsTable("edureka_dw.profitable_products_per_month")

    top_10_selling_product_df = orders\
                                    .groupBy("productid")\
                                    .agg({"customerid":"count"})\
                                    .withColumnRenamed("count(customerid)","customer_count")\
                                    .withColumn("rnk",dense_rank().over(Window.orderBy(desc("customer_count"))))\
                                    .filter(col("rnk")<=10)\
                                    .select("productid")\
                                    .dropDuplicates(["productid"])

    top_10_selling_product_df.show()

    product_recommendation_stg = orders.alias("o")\
                                        .join(top_10_selling_product_df.alias("top_10_products"),top_10_selling_product_df.productid==orders.productid,"inner")\
                                        .drop(top_10_selling_product_df.productid)\
                                        .select("id","o.productid","customerid")




    product_recommendation_stg_1 = product_recommendation_stg.alias("stg")\
                                                             .join(orders.alias("p").withColumnRenamed("productid","r_product_id"), ["customerid"], "left")\
                                                             .where(expr("productid <> r_product_id"))\
                                                             .groupBy("productid","r_product_id")\
                                                             .agg({"*":"count"})\
                                                             .withColumnRenamed("count(1)","cnt") \
                                                             .withColumnRenamed("productid", "productid") \
                                                             .withColumnRenamed("r_product_id", "recommended_product_id") \
                                                             .withColumn("rn", row_number().over(Window.partitionBy("productid").orderBy(desc("cnt")))) \
                                                             .filter(col("rn") <= 3)

    product_recommendation_final = product_recommendation_stg_1.alias("stg") \
        .join(product.alias("prod"), product_recommendation_stg_1.productid == product.id, "left") \
        .join(product_r.alias("prod1"), product_recommendation_stg_1.recommended_product_id == product_r.r_id, "left") \
        .groupBy("productid", "productname") \
        .agg(func.collect_list("recommended_product_id"), func.collect_list("r_productname"))


    product_recommendation_final.write.format("orc").mode("overwrite").saveAsTable("edureka_dw.product_recommendations")
