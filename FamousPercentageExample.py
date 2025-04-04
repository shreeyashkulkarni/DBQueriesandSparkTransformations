"""
𝐌𝐮𝐬𝐭 𝐓𝐫𝐲: Meta/Facebook (Hard Level) hashtag#SQL Interview Question — Solution

A table named “famous” has two columns called user id and follower id. It represents each user ID has a particular follower ID. These follower IDs are also users of hashtag#Facebook / hashtag#Meta. Then, find the famous percentage of each user.
Famous Percentage = number of followers a user has / total number of users on the platform.

🔍 At first glance, this might seem tedious, but it's straightforward once you break it down. Give it a try! 👇

𝐒𝐜𝐡𝐞𝐦𝐚 𝐚𝐧𝐝 𝐃𝐚𝐭𝐚𝐬𝐞𝐭:
CREATE TABLE famous (user_id INT, follower_id INT);

INSERT INTO famous VALUES
(1, 2), (1, 3), (2, 4), (5, 1), (5, 3),
(11, 7), (12, 8), (13, 5), (13, 10),
(14, 12), (14, 3), (15, 14), (15, 13);
-------------

Solution :

                    with distinct_users AS(

                                    SELECT user_id  As users from famous
                                    UNION
                                    SELECT follower_id As users from famous
                    ),
                    follower_count As(

                                        SELECT user_id,
                                        count(follower_id) as followers
                                        from
                                        famous
                                        group by
                                        user_id
                    )
                    select
                        f.user_id,
                        (f.followers * 100.0) / (select count(*) from distinct_users) As famous_percentage
                    from
                    follower_count as f;
"""




from pyspark.sql import SparkSession,Row
from pyspark.sql.functions import col,lit,count

import logging

logging.basicConfig(level=logging.INFO,format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


if __name__ == '__main__':

    logger.info("Started Spark Application")

    try:
        spark = SparkSession.builder.appName("Famous-Percentage")\
                .config("spark.ui.port","4041")\
                .master("local[*]")\
                .getOrCreate()

        spark.sparkContext.setLogLevel("WARN")
        logger.info("Spark Session formed Successfully")
    except Exception as e:
        logger.error("Failed to form Spark Session",exec_info=True)
        raise


    df = spark.read.option("header",True)\
        .format("csv")\
        .load("file:///C://Users/shree/PycharmProjects/TransformationsFromQueries/INPUT/csv_famous_percentage/famous_202503310331.csv")

    logger.info("Table - Famous")

    df.show(truncate=False)


    distinct_users = df.select(col("user_id").alias("users"))\
                        .union(
                                df.select(col("follower_id").alias("users"))
                                ).distinct()



    logger.info("Distinct Users")

    distinct_users.show(truncate=False)


    total_users_count = distinct_users.count()

    logger.info("Total Unique users on the platform : "+str(total_users_count))

    follower_count = df.groupBy(col("user_id")).agg(count(col("follower_id")).alias("followers"))

    logger.info("Follower Count : ")

    follower_count.orderBy(col("user_id").cast("int").asc()).show(truncate=False)

    logger.info("Total follower count for each user : "+str(follower_count.collect()[0][1]))

    famous_percentage_df = follower_count.withColumn(
        "famous_percentage",
        (col("followers") * 100.0)/(lit(total_users_count))

    )

    famous_percentage_df.orderBy(col("famous_percentage").desc()).show()

    logger.info("Saving File : C://Users/shree/PycharmProjects/TransformationsFromQueries/output-csv-famous-percentage/")

    famous_percentage_df.repartition(1).write\
        .option("header",True)\
        .mode("overwrite")\
        .format("csv")\
        .save("file:///C://Users/shree/PycharmProjects/TransformationsFromQueries/output-csv-famous-percentage/")



    logger.info("End Spark Application")


