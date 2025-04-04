"""
𝐌𝐮𝐬𝐭 𝐓𝐫𝐲: Google (Medium Level) hashtag#SQL Interview Question — Solution

You are analyzing a social network dataset at Google. Your task is to find mutual friends between two users, Karl and Hans. There is only one user named Karl and one named Hans in the dataset.

The output should contain 'user_id' and 'user_name' columns.

🔗 Understanding how to join tables in SQL is essential for effective data analysis; mastering this concept allows you to combine related data seamlessly. Give it a try! 👇

𝐒𝐜𝐡𝐞𝐦𝐚 𝐚𝐧𝐝 𝐃𝐚𝐭𝐚𝐬𝐞𝐭:
CREATE TABLE users(user_id INT, user_name varchar(30));
INSERT INTO users VALUES (1, 'Karl'), (2, 'Hans'), (3, 'Emma'), (4, 'Emma'), (5, 'Mike'), (6, 'Lucas'), (7, 'Sarah'), (8, 'Lucas'), (9, 'Anna'), (10, 'John');

CREATE TABLE friends(user_id INT, friend_id INT);
INSERT INTO friends VALUES (1,3),(1,5),(2,3),(2,4),(3,1),(3,2),(3,6),(4,7),(5,8),(6,9),(7,10),(8,6),(9,10),(10,7),(10,9);
-------------

Solution :

            SELECT distinct u.user_id, u.user_name
            FROM users u
            JOIN friends f1 ON u.user_id = f1.friend_id  -- Karl's friends
            JOIN friends f2 ON u.user_id = f2.friend_id  -- Hans's friends
            WHERE f1.user_id = (SELECT user_id FROM users WHERE user_name = 'Karl')
            AND f2.user_id = (SELECT user_id FROM users WHERE user_name = 'Hans');

"""

import logging
from pyspark.sql import SparkSession,Row
from pyspark.sql.functions import col,lit

logging.basicConfig(level=logging.INFO,format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)



if __name__ == '__main__':

    logger.info("Spark Application Start")

    try:
        spark = SparkSession.builder.appName("Mutual-Friends-Karl-Hans")\
                .config("spark.ui.port","4041")\
                .master("local[*]")\
                .getOrCreate()

        logger.info("Successfully formed Spark Session")
        spark.sparkContext.setLogLevel("WARN")
    except Exception as e:
        logger.error("Failed to form the Spark Session",exec_info=True)
        raise



    users_df = spark.read\
                .option("header",True)\
                .option("inferschema",True)\
                .option("mode","PERMISSIVE")\
                .format("csv")\
                .load("file:///C://Users/shree/PycharmProjects/TransformationsFromQueries/INPUT/csv_mutual_karl_Hans/users*.csv")

    logger.info("Users DataFrame")

    users_df.show(truncate=False)


    friends_df = spark.read\
                .option("header",True)\
                .option("inferschema",True)\
                .option("mode","PERMISSIVE")\
                .format("csv")\
                .load("file:///C://Users/shree/PycharmProjects/TransformationsFromQueries/INPUT/csv_mutual_karl_Hans/friends*.csv")

    logger.info("Friends DataFrame")

    friends_df.show(truncate=False)


    joined_df = users_df.alias("u").join(friends_df.alias("f1"),col("u.user_id") == col("f1.friend_id"),"inner")\
                .join(friends_df.alias("f2"),col("u.user_id") == col("f2.friend_id"),"inner")
    joined_df = joined_df.filter( (col("f1.user_id") == (users_df.filter(col("user_name") == 'Karl').select("user_id").collect()[0][0]))
                                  &
                                  (col("f2.user_id") == (users_df.filter(col("user_name") == 'Hans').select("user_id").collect()[0][0]))
                                  )
    joined_df = joined_df.distinct().select(col("u.user_id"),col("u.user_name"))
    joined_df.show(truncate=False)

    logger.info(
        "Saving File : file:///C://Users/shree/PycharmProjects/TransformationsFromQueries/output-csv-mutual-friends/")

    joined_df.repartition(1).write\
    .option("header",True)\
    .format("csv")\
    .mode("overwrite")\
    .save("file:///C://Users/shree/PycharmProjects/TransformationsFromQueries/output-csv-mutual-friends/")






    logger.info("Spark Application End")