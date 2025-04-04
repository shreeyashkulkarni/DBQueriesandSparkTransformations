"""
𝐌𝐮𝐬𝐭 𝐓𝐫𝐲: Uber(Hard Level) hashtag#SQL Interview Question — Solution

Some forecasting methods are extremely simple and surprisingly effective. Naïve forecast is one of them. To create a naïve forecast for "distance per dollar" (defined as distance_to_travel/monetary_cost), first sum the "distance to travel" and "monetary cost" values monthly. This gives the actual value for the current month. For the forecasted value, use the previous month's value. After obtaining both actual and forecasted values, calculate the root mean squared error (RMSE) using the formula RMSE = sqrt(mean(square(actual - forecast))). Report the RMSE rounded to two decimal places.

🔍 At first glance, this might seem tedious, but it's straightforward once you break it down. Give it a try! 👇

𝐒𝐜𝐡𝐞𝐦𝐚 𝐚𝐧𝐝 𝐃𝐚𝐭𝐚𝐬𝐞𝐭:
CREATE TABLE uber_request_logs(request_id int, request_date datetime, request_status varchar(10), distance_to_travel float, monetary_cost float, driver_to_client_distance float);

INSERT INTO uber_request_logs VALUES (1,'2020-01-09','success', 70.59, 6.56,14.36), (2,'2020-01-24','success', 93.36, 22.68,19.9), (3,'2020-02-08','fail', 51.24, 11.39,21.32), (4,'2020-02-23','success', 61.58,8.04,44.26), (5,'2020-03-09','success', 25.04,7.19,1.74), (6,'2020-03-24','fail', 45.57, 4.68,24.19), (7,'2020-04-08','success', 24.45,12.69,15.91), (8,'2020-04-23','success', 48.22,11.2,48.82), (9,'2020-05-08','success', 56.63,4.04,16.08), (10,'2020-05-23','fail', 19.03,16.65,11.22), (11,'2020-06-07','fail', 81,6.56,26.6), (12,'2020-06-22','fail', 21.32,8.86,28.57), (13,'2020-07-07','fail', 14.74,17.76,19.33), (14,'2020-07-22','success',66.73,13.68,14.07), (15,'2020-08-06','success',32.98,16.17,25.34), (16,'2020-08-21','success',46.49,1.84,41.9), (17,'2020-09-05','fail', 45.98,12.2,2.46), (18,'2020-09-20','success',3.14,24.8,36.6), (19,'2020-10-05','success',75.33,23.04,29.99), (20,'2020-10-20','success', 53.76,22.94,18.74);
--------------

Solution :


                with monthly_sums_of_distance_and_costs as(

                            select
                                TO_CHAR(request_date::timestamp, 'YYYY-MM') as request_date,
                                sum(distance_to_travel) as total_distance,
                                sum(monetary_cost) as total_cost
                            from
                                uber_request_logs
                            group by
                                TO_CHAR(request_date::timestamp, 'YYYY-MM')

                ),
                distance_per_dollar As(

                            select
                            request_date,
                            ((total_distance * 1.0)/total_cost)	as	actual_value
                            from
                            monthly_sums_of_distance_and_costs

                ),
                naive_forecast As (

                        select
                        *,
                        Lag(actual_value,1) OVER(order by request_date) as forecasted_value
                        from
                        distance_per_dollar
                )
                select
                    ROUND(CAST(SQRT(AVG(POWER((actual_value - forecasted_value), 2))) AS NUMERIC), 2) AS RMSE
                from
                naive_forecast;

"""

import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,lit,date_format,sum,lag,avg,sqrt,round
from pyspark.sql.window import Window


logging.basicConfig(level=logging.INFO,format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


if __name__ == '__main__':

    logger.info("Spark Application Start")

    try:


        spark = SparkSession.builder.appName("Spark-App")\
                .config("spark.ui.port","4041")\
                .master("local[*]")\
                .getOrCreate()

        logger.info("Spark Session created Successfully!!")

        spark.sparkContext.setLogLevel("WARN")

    except Exception as e:
        logger.error("Failed to create Spark Session",exec_info=True)
        raise


    df = spark.read.option("header",True)\
         .option("inferschema",True)\
         .format("csv")\
         .load("file:///C://Users/shree/PycharmProjects/TransformationsFromQueries/INPUT/csv_Naive_Forecast/uber_request_logs_202504032256.csv")


    df.show(truncate=False)

    monthly_sums_of_distance_and_costs_df = df.withColumn("request_date",date_format(col("request_date"),"yyyy-MM"))
    monthly_sums_of_distance_and_costs_df = monthly_sums_of_distance_and_costs_df.groupBy(col("request_date"))\
                                            .agg(sum(col("distance_to_travel")).alias("total_distance"),
                                                 sum(col("monetary_cost")).alias("total_cost"))


    monthly_sums_of_distance_and_costs_df.show(truncate=False)

    distance_per_dollar_df = monthly_sums_of_distance_and_costs_df\
                            .withColumn("Actual_Value",col("total_distance")/col("total_cost"))\
                            .select("request_date","Actual_Value")

    distance_per_dollar_df.show(truncate=False)

    windowspec = Window.orderBy(col("request_date").asc())

    naive_forecast_df = distance_per_dollar_df\
                        .withColumn("forecasted_value",lag(col("Actual_Value"),1).over(windowspec))

    naive_forecast_df.show(truncate=False)


    final_df  = naive_forecast_df.agg(
                                                round(sqrt(avg((col("Actual_Value") - col("forecasted_value"))**2)),2).alias("RMSE")

                                     )

    final_df.select("RMSE").show(truncate=False)


    logger.info("Saving file to file:///C://Users/shree/PycharmProjects/TransformationsFromQueries/output-csv-naive-forecast/")

    final_df.repartition(1).write\
            .option("header",True)\
            .option("InferSchema",True)\
            .format("csv")\
            .mode("overwrite")\
            .save("file:///C://Users/shree/PycharmProjects/TransformationsFromQueries/output-csv-naive-forecast/")



    logger.info("Spark Application End")











    

