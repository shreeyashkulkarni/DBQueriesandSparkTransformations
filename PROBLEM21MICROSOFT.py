"""
𝐌𝐮𝐬𝐭 𝐓𝐫𝐲: Microsoft (Hard Level) #SQL Interview Question — Solution

Find the total number of downloads for paying and non-paying users by date. Include only records where non-paying customers have more downloads than paying customers. The output should be sorted by earliest date first and contain 3 columns date, non-paying downloads, paying downloads. 

Note: In Oracle you should use "date" when referring to date column (reserved keyword).

🔍By solving this, you'll learn how to use join, groupby and having. Give it a try and share the output!👇

𝐒𝐜𝐡𝐞𝐦𝐚 𝐚𝐧𝐝 𝐃𝐚𝐭𝐚𝐬𝐞𝐭:
CREATE TABLE ms_user_dimension (user_id INT PRIMARY KEY,acc_id INT);
INSERT INTO ms_user_dimension (user_id, acc_id) VALUES (1, 101),(2, 102),(3, 103),(4, 104),(5, 105);

CREATE TABLE ms_acc_dimension (acc_id INT PRIMARY KEY,paying_customer VARCHAR(10));
INSERT INTO ms_acc_dimension (acc_id, paying_customer) VALUES (101, 'Yes'),(102, 'No'),(103, 'Yes'),(104, 'No'),(105, 'No');

CREATE TABLE ms_download_facts (date DATETIME,user_id INT,downloads INT);
INSERT INTO ms_download_facts (date, user_id, downloads) VALUES ('2024-10-01', 1, 10),('2024-10-01', 2, 15),('2024-10-02', 1, 8),('2024-10-02', 3, 12),('2024-10-02', 4, 20),('2024-10-03', 2, 25),('2024-10-03', 5, 18);
-----------

Solution : 
            select 
                    date , 
                    non_paying_downloads,
                    paying_downloads
            from (
                        select 
                                date,
                                coalesce(sum(downloads) filter(where mad.paying_customer = 'No'),0) as non_paying_downloads,
                                coalesce(sum(downloads) filter(where mad.paying_customer = 'Yes'),0) as paying_downloads
                        from ms_user_dimension mud inner join ms_acc_dimension mad on mud.acc_id = mad.acc_id  
                        inner join ms_download_facts as mdf on mud.user_id = mdf.user_id 
                        group by date
                        order by date asc
                ) as sub_query
            where 
            non_paying_downloads > paying_downloads;


"""


from pyspark.sql import SparkSession
from pyspark.sql.functions import col,lit,sum,coalesce,when
import logging


logging.basicConfig(level = logging.INFO,format = "%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


if __name__ == "__main__":

    logger.info("Start Spark Application")


    try:


        spark = SparkSession.builder.appName("Spark-ProblemNumber21")\
                .config("spark.ui.port","4041")\
                .config("spark.jars","file:///C://Users/shree/Downloads/postgresql-42.7.4.jar")\
                .master("local[*]").getOrCreate()

        spark.sparkContext.setLogLevel("WARN")


    except Exception as e :
        logger.error("Failed to create Spark Session",exec_info = True)
        raise



    jdbc_url = "jdbc:postgresql://localhost:5432/postgres"
    ms_user_dimension_query = "(select * from ms_user_dimension) AS query"
    connection_properties = {
        "user":"postgres",
        "password":"Pkts1t4j11@",
        "driver":"org.postgresql.Driver"
    }

    ms_user_dimension_df = spark.read.jdbc(
                                                url = jdbc_url,
                                                table=ms_user_dimension_query,
                                                properties=connection_properties
    )

    
    ms_acc_dimension_query = "(select * from ms_acc_dimension) AS query"

    ms_acc_dimension_df = spark.read.jdbc(
                                                url = jdbc_url,
                                                table=ms_acc_dimension_query,
                                                properties=connection_properties
    )

    ms_download_facts_query = "(select * from ms_download_facts) AS query"

    ms_download_facts_df = spark.read.jdbc(
                                                url = jdbc_url,
                                                table=ms_download_facts_query,
                                                properties=connection_properties
    )        


    ms_user_dimension_df.show(truncate=False)
    ms_acc_dimension_df.show(truncate=False)
    ms_download_facts_df.show(truncate=False)


    joined_df = ms_user_dimension_df.alias("mud")\
                .join(ms_acc_dimension_df.alias("mad"),col("mud.acc_id") == col("mad.acc_id"))\
                .join(ms_download_facts_df.alias("mdf"),col("mud.user_id") == col("mdf.user_id"))
    
    
    
    aggregateddf = joined_df.groupBy(col("mdf.date")).agg(

        coalesce(sum(
            when(col("mad.paying_customer") == "No",
            col("mdf.downloads"))
            ),lit(0)).alias("non_paying_downloads"),

        coalesce(sum(
            when(col("mad.paying_customer") == "Yes",
            col("mdf.downloads"))
        ),lit(0)).alias("paying_downloads")
    )

    final_df = aggregateddf.select(col("date"),col("non_paying_downloads"),col("paying_downloads"))\
        .where(
                    col("non_paying_downloads") > col("paying_downloads")
            )
    
    final_df.show(truncate=False)


    logger.info("End Spark Application")

