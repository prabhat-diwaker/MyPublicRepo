from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
import pyspark.sql.functions as func
from pyspark.sql.window import Window
from pyspark.sql.functions import lead
import pyspark

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


    movies_df = spark.read.table("edureka_dw.movies")
    theatre_df = spark.read.table("edureka_dw.theatre")
    seat_bookings_df = spark.read.table("edureka_dw.seat_bookings")


    seat_bookings_df_03 = seat_bookings_df.filter(func.col("movie_date")=="2019-11-03").dropDuplicates(["theatre_id"])
    theatre_df_03 = theatre_df.join(seat_bookings_df_03,seat_bookings_df_03.theatre_id==theatre_df.id,"inner")\
                              .drop(seat_bookings_df_03.row)\
                              .select("id",
                                      "theatre_name",
                                      "row",
                                      "total_seats_in_row"
                                      )

    seat_booking_theatre_3_df = seat_bookings_df.alias("sb").filter(func.col("movie_date")=="2019-11-03")\
                    .join(theatre_df_03.alias("t"),(theatre_df_03.id ==seat_bookings_df.theatre_id) &(theatre_df_03.row ==seat_bookings_df.row),"right" )\
                    .drop(seat_bookings_df.row)\
                    .select("seat_no",
                            expr("coalesce(movie_date,'2019-11-03') as movie_date"),
                            "id",
                            "theatre_name",
                            "t.row",
                            "total_seats_in_row")\
                    .withColumn("next_seat",lead("seat_no",1,0).over(Window.partitionBy("id","row","movie_date").orderBy("seat_no")))

    #seat_booking_theatre_3_df.count()

    seat_bookings_df_04 = seat_bookings_df.filter(func.col("movie_date")=="2019-11-04").dropDuplicates(["theatre_id"])
    theatre_df_04 = theatre_df.join(seat_bookings_df_04,seat_bookings_df_04.theatre_id==theatre_df.id,"inner")\
                              .drop(seat_bookings_df_04.row)\
                              .select("id",
                                      "theatre_name",
                                      "row",
                                      "total_seats_in_row"
                                      )

    seat_booking_theatre_4_df = seat_bookings_df.alias("sb").filter(func.col("movie_date")=="2019-11-04")\
                    .join(theatre_df_04.alias("t"),(theatre_df_04.id ==seat_bookings_df.theatre_id) &(theatre_df_04.row ==seat_bookings_df.row),"right" )\
                    .drop(seat_bookings_df.row)\
                    .select("seat_no",
                            expr("coalesce(movie_date,'2019-11-04') as movie_date"),
                            "id",
                            "theatre_name",
                            "t.row",
                            "total_seats_in_row")\
                    .withColumn("next_seat",lead("seat_no",1,0).over(Window.partitionBy("id","row","movie_date").orderBy("seat_no")))

    # seat_booking_theatre_3_df.count()

    seat_bookings_df_05 = seat_bookings_df.filter(func.col("movie_date") == "2019-11-05").dropDuplicates(["theatre_id"])
    theatre_df_05 = theatre_df.join(seat_bookings_df_05, seat_bookings_df_05.theatre_id == theatre_df.id, "inner") \
        .drop(seat_bookings_df_05.row) \
        .select("id",
                "theatre_name",
                "row",
                "total_seats_in_row"
                )

    seat_booking_theatre_5_df = seat_bookings_df.alias("sb").filter(func.col("movie_date") == "2019-11-05") \
        .join(theatre_df_05.alias("t"),
              (theatre_df_05.id == seat_bookings_df.theatre_id) & (theatre_df_05.row == seat_bookings_df.row), "right") \
        .drop(seat_bookings_df.row) \
        .select("seat_no",
                expr("coalesce(movie_date,'2019-11-05') as movie_date"),
                "id",
                "theatre_name",
                "t.row",
                "total_seats_in_row") \
        .withColumn("next_seat",
                    lead("seat_no", 1, 0).over(Window.partitionBy("id", "row", "movie_date").orderBy("seat_no")))


    stage_df =seat_booking_theatre_3_df.unionAll(seat_booking_theatre_4_df).unionAll(seat_booking_theatre_5_df)\

    final_df = stage_df.join(movies_df, movies_df.theatre_id == stage_df.id, "inner") \
        .select("movie_date",
                expr("id as theatre_id"),
                "theatre_name",
                "row",
                "seat_no",
                "next_seat",
                "total_seats_in_row",
                expr("""case 
     when next_seat <> 0 then (next_seat-seat_no)-1 
     when next_seat=0 and total_seats_in_row <> seat_no then (total_seats_in_row-seat_no) 
     when next_seat=0 and total_seats_in_row = seat_no then (total_seats_in_row-seat_no)
     when seat_no is null  then total_seats_in_row
    end as empty_seat"""),
                     "movie_id",
                     "movie_name"
                )\
        .filter(func.col("empty_seat")>=3)\
        .dropDuplicates(["movie_date","theatre_id","theatre_name","row","seat_no","next_seat","total_seats_in_row","empty_seat","movie_id","movie_name"])\
        .orderBy("movie_date","theatre_name","row","seat_no")
    

    final_df.write.format("orc").mode("overwrite").saveAsTable("edureka_dw.empty_seats")

