"""
𝐌𝐮𝐬𝐭 𝐓𝐫𝐲: Amazon (Hard Level) hashtag#SQL Interview Question — Solution

Given a table 'sf_transactions' of purchases by date, calculate the month-over-month percentage change in revenue. The output should include the year-month date (YYYY-MM) and percentage change, rounded to the 2nd decimal point, and sorted from the beginning of the year to the end of the year. The percentage change column will be populated from the 2nd month forward and calculated as ((this month’s revenue — last month’s revenue) / last month’s revenue)*100.

⏳ Dealing with dates in SQL is really important; mastering this skill gives you valuable exposure.Give it a try! 👇

𝐒𝐜𝐡𝐞𝐦𝐚 𝐚𝐧𝐝 𝐃𝐚𝐭𝐚𝐬𝐞𝐭:
CREATE TABLE sf_transactions(id INT, created_at datetime, value INT, purchase_id INT);

INSERT INTO sf_transactions VALUES
(1, '2019-01-01 00:00:00',  172692, 43), (2,'2019-01-05 00:00:00',  177194, 36),(3, '2019-01-09 00:00:00',  109513, 30),(4, '2019-01-13 00:00:00',  164911, 30),(5, '2019-01-17 00:00:00',  198872, 39), (6, '2019-01-21 00:00:00',  184853, 31),(7, '2019-01-25 00:00:00',  186817, 26), (8, '2019-01-29 00:00:00',  137784, 22),(9, '2019-02-02 00:00:00',  140032, 25), (10, '2019-02-06 00:00:00', 116948, 43), (11, '2019-02-10 00:00:00', 162515, 25), (12, '2019-02-14 00:00:00', 114256, 12), (13, '2019-02-18 00:00:00', 197465, 48), (14, '2019-02-22 00:00:00', 120741, 20), (15, '2019-02-26 00:00:00', 100074, 49), (16, '2019-03-02 00:00:00', 157548, 19), (17, '2019-03-06 00:00:00', 105506, 16), (18, '2019-03-10 00:00:00', 189351, 46), (19, '2019-03-14 00:00:00', 191231, 29), (20, '2019-03-18 00:00:00', 120575, 44), (21, '2019-03-22 00:00:00', 151688, 47), (22, '2019-03-26 00:00:00', 102327, 18), (23, '2019-03-30 00:00:00', 156147, 25);
-------------


Solution :

                        with total_months_revenue As(

                                        select
                                        --DATE_PART('year',created_at) || '-' || DATE_PART('month',created_at) as t_date,
                                        TO_CHAR(created_at::timestamp,'YYYY-MM') as t_date,
                                        SUM(value) as tot_rev
                                        from sf_transactions st
                                        group by
                        --				DATE_PART('year',created_at),
                        --				DATE_PART('month',created_at)
                                        TO_CHAR(created_at::timestamp,'YYYY-MM')
                                        order by t_date



                        ),
                        prev_rev As (

                                        select
                                        *,
                                        Lag(tot_rev,1) OVER(ORDER By t_date) as previous_revenue
                                        from
                                        total_months_revenue
                                        order by t_date


                        )

                        select
                        t_date,
                        tot_rev as Total_Revenue,

                        ROUND(
                                CASE
                                    WHEN previous_revenue is NULL THEN NULL
                                    ELSE (((tot_rev - previous_revenue) * 100.0) / previous_revenue)
                                END,2




                        )as percentage_change


                        --(((tot_rev - previous_revenue) * 100.0) / previous_revenue) as percentage_change
                        from
                        prev_rev;


"""

import logging
from pyspark.sql import SparkSession,Row
from pyspark.sql.functions import col,lit,date_format,sum,lag,coalesce,round
from pyspark.sql.window import Window


logging.basicConfig(level=logging.INFO,format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()


if __name__ == '__main__':

    logger.info("Start Application")


    try :

        spark = SparkSession.builder.appName("month-over-month-prct-change")\
                .config("spark.ui.port","4041")\
                .master("local[*]")\
                .getOrCreate()

        logger.info("Successfully Created Spark Session")
        # spark.sparkContext.setLogLevel("INFO")

    except Exception as e:
        logger.error("Failed to create Spark Session",exec_info=True)
        raise


    df = spark.read.option("header",True)\
        .format("csv").load("file:///C://Users/shree/PycharmProjects/TransformationsFromQueries/INPUT/csv_sf_transactions/sf_transactions_202503312309.csv")

    df.show(truncate=False)

    total_months_revenue = df.withColumn("created_at",date_format("created_at",'yyyy-MM'))
    total_months_revenue = total_months_revenue.groupBy("created_at").agg(sum(col("value")).alias("Total_Revenue")).orderBy(col("created_at"))
    total_months_revenue.show(truncate=False)


    windowspec = Window.orderBy(col("created_at"))
    prev_rev_df = total_months_revenue.withColumn("Previous_Revenue",
                                               lag("Total_Revenue",1).over(windowspec)
                                               )
    prev_rev_df = prev_rev_df.withColumn("Previous_Revenue",coalesce("Previous_Revenue",lit(0)))
    prev_rev_df.show(truncate=False)


    final_df = prev_rev_df.withColumn("Percentage_change",
                                      round(((col("Total_Revenue")-col("Previous_Revenue"))*100.0)/col("Previous_Revenue"),2)
                                      )\
                            .drop(col("Previous_Revenue"))

    final_df.show(truncate=False)

    logger.info("Saving File : file:///C://Users/shree/PycharmProjects/TransformationsFromQueries/output-csv-month-prct_chng/")

    final_df.repartition(1).write\
    .option("header",True)\
    .format("csv")\
    .mode("overwrite")\
    .save("file:///C://Users/shree/PycharmProjects/TransformationsFromQueries/output-csv-month-prct_chng/")


    logger.info("End Application")
