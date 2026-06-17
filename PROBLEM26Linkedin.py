"""
Consider all LinkedIn users who, at some point, worked at Microsoft. For how many of them was Google their next employer right after Microsoft (no employers in between)?

🌀 Trust me, this one will seriously twist your brain! Give it a try and share the output! 👇

𝐒𝐜𝐡𝐞𝐦𝐚 𝐚𝐧𝐝 𝐃𝐚𝐭𝐚𝐬𝐞𝐭:
CREATE TABLE linkedin_users (user_id INT,employer VARCHAR(255),position VARCHAR(255),start_date DATETIME,end_date DATETIME);

INSERT INTO linkedin_users (user_id, employer, position, start_date, end_date) VALUES(1, 'Microsoft', 'developer', '2020-04-13', '2021-11-01'),(1, 'Google', 'developer', '2021-11-01', NULL),(2, 'Google', 'manager', '2021-01-01', '2021-01-11'),(2, 'Microsoft', 'manager', '2021-01-11', NULL),(3, 'Microsoft', 'analyst', '2019-03-15', '2020-07-24'),(3, 'Amazon', 'analyst', '2020-08-01', '2020-11-01'),(3, 'Google', 'senior analyst', '2020-11-01', '2021-03-04'),(4, 'Google', 'junior developer', '2018-06-01', '2021-11-01'),(4, 'Google', 'senior developer', '2021-11-01', NULL),(5, 'Microsoft', 'manager', '2017-09-26', NULL),(6, 'Google', 'CEO', '2015-10-02', NULL);

-----------

Solution : 

            with mycte AS (
					SELECT 
							*,
							LEAD(employer) OVER(PARTITION BY user_id ORDER BY start_date asc) as next_employer
					FROM
							linkedin_users lu 
)
select count(*) from mycte where employer = 'Microsoft' and next_employer = 'Google';

"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col,lead,count
import logging
from pyspark.sql.window import Window



logging.basicConfig(level = logging.INFO,format = "%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)



if __name__ == "__main__":
    logger.info("Starting Spark Application")


    try:
        spark = SparkSession.builder.appName("Spark- Problem26Linkedin")\
                                    .config("spark.ui.port","4041")\
                                    .config("spark.jars","file:///C://Users/shree/Downloads/postgresql-42.7.4.jar")\
                                    .master("local[*]")\
                                    .getOrCreate()

        spark.sparkContext.setLogLevel("WARN")


    except Exception as e:
        logger.error("Failed to create Spark Session ",exec_info = True)
        raise



    

    jdbc_url = "jdbc:postgresql://localhost:5432/postgres"
    query = "(select * from linkedin_users) AS query"
    connection_properties = {
            "user" : "postgres",
            "password": "Pkts1t4j11@",
            "driver" : "org.postgresql.Driver"
    }


    linkedin_users_df = spark.read.jdbc(url = jdbc_url , table=query , properties=connection_properties)

    linkedin_users_df.show(truncate=False)

    windowspec = Window.partitionBy(col("user_id")).orderBy(col("start_date").asc())


    find_next_employer_df = linkedin_users_df.withColumn("next_employer",lead(col("employer")).over(windowspec))
    find_next_employer_df.show(truncate=False)

    final_df = find_next_employer_df.filter((col("employer") == "Microsoft" ) & (col("next_employer") == "Google"))
    final_df.show(truncate=False)


    logger.info("Ending Spark Application")