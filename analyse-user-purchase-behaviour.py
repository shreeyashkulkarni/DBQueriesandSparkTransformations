"""
𝐌𝐮𝐬𝐭 𝐓𝐫𝐲: IBM(Hard Level) hashtag#SQL Interview Question — Solution

IBM is working on a new feature to analyze user purchasing behavior for all Fridays in the first quarter of the year. For each Friday separately, calculate the average amount users have spent per order. The output should contain the week number of that Friday and average amount spent.

🔍 By solving this, you'll learn how to handle date and time by the end of the day. Give it a try! 👇

𝐒𝐜𝐡𝐞𝐦𝐚 𝐚𝐧𝐝 𝐃𝐚𝐭𝐚𝐬𝐞𝐭:
CREATE TABLE user_purchases(user_id int, date date, amount_spent float, day_name varchar(15));

INSERT INTO user_purchases VALUES(1047,'2023-01-01',288,'Sunday'),(1099,'2023-01-04',803,'Wednesday'),(1055,'2023-01-07',546,'Saturday'),(1040,'2023-01-10',680,'Tuesday'),(1052,'2023-01-13',889,'Friday'),(1052,'2023-01-13',596,'Friday'),(1016,'2023-01-16',960,'Monday'),(1023,'2023-01-17',861,'Tuesday'),(1010,'2023-01-19',758,'Thursday'),(1013,'2023-01-19',346,'Thursday'),(1069,'2023-01-21',541,'Saturday'),(1030,'2023-01-22',175,'Sunday'),(1034,'2023-01-23',707,'Monday'),(1019,'2023-01-25',253,'Wednesday'),(1052,'2023-01-25',868,'Wednesday'),(1095,'2023-01-27',424,'Friday'),(1017,'2023-01-28',755,'Saturday'),(1010,'2023-01-29',615,'Sunday'),(1063,'2023-01-31',534,'Tuesday'),(1019,'2023-02-03',185,'Friday'),(1019,'2023-02-03',995,'Friday'),(1092,'2023-02-06',796,'Monday'),(1058,'2023-02-09',384,'Thursday'),(1055,'2023-02-12',319,'Sunday'),(1090,'2023-02-15',168,'Wednesday'),(1090,'2023-02-18',146,'Saturday'),(1062,'2023-02-21',193,'Tuesday'),(1023,'2023-02-24',259,'Friday');
-------------


solution  :

                with first_quarter_records As(

                                    select
                                        *
                                    from
                                    user_purchases
                                    where
                                    DATE_PART('month',date) between 1 AND 4
                            )

                                select
                                    date_part('week',date) as weeknumber,
                                    ROUND(CAST((sum(amount_spent) * 1.0/ COUNT(user_id)) AS NUMERIC),2) as avg_amount_per_user
                                from
                                    first_quarter_records
                                where
                                    day_name = 'Friday'
                                group by
                                    date,day_name;

"""



import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,lit,month,weekofyear,sum,count,round



logging.basicConfig(level=logging.INFO,format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


if __name__=='__main__':

    logger.info("Spark Application Start")


    # try:
    #     spark = SparkSession.builder.appName("analyse-user-purchase-behaviour")\
    #             .config("spark.ui.port","4041")\
    #             .config("spark.jars","file:///C://Users/shree/Downloads/postgresql-42.7.4.jar")\
    #             .master("local[*]")\
    #             .getOrCreate()
    #
    #     spark.sparkContext.setLogLevel("WARN")
    #
    #     logger.info("Spark Session created Successfully!!!")
    #
    # except Exception as e:
    #     logger.error("Failed to Create Sparksession")
    #     raise


    dict = {"a":"shr","b":"k"}



    dict["a"] = "xyz"

    print(dict)


    tp = (1,2,3,4,5)



    print(tp)





    # jdbc_url  = "jdbc:postgresql://localhost:5432/postgres"
    #
    # query = "(select * from user_purchases) AS query"
    #
    # connection_properties = {
    #
    #     "user":"postgres",
    #     "password":"Pkts1t4j11@",
    #     "driver":"org.postgresql.Driver"
    #
    #
    # }
    #
    # user_purchases_df = spark.read.jdbc(url=jdbc_url,table=query,properties=connection_properties)
    # user_purchases_df.show()
    #
    # first_quarter_records_df = user_purchases_df\
    #                             .withColumn("month",month(col("date")))\
    #                             .filter((col("month") >= 1) & (col("month") <= 4))
    #
    # first_quarter_records_df.show()
    #
    #
    # final_df = first_quarter_records_df\
    #             .groupBy(col("date"),col("day_name"))\
    #             .agg(weekofyear(col("date")).alias("WeekNumber"),(round(sum(col("amount_spent"))/count(col("user_id")),2)).alias("avg_amount_per_user"))\
    #             .where(col("day_name") == 'Friday')\
    #             .select(col("WeekNumber"),col("avg_amount_per_user"))
    #
    # final_df.show()
    #
    # final_df.repartition(1).write\
    # .option("header",True)\
    # .option("mode","overwrite")\
    # .format("parquet")\
    # .save("file:///C://Users/shree/PycharmProjects/TransformationsFromQueries/output-csv-analyse-user-purchase-behaviour")


    logger.info("Spark Application End")