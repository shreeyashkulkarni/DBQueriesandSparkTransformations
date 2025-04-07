"""
𝐌𝐮𝐬𝐭 𝐓𝐫𝐲: Airbnb(Medium Level) hashtag#SQL Interview Question — Solution

Find the total number of available beds per hosts' nationality.
Output the nationality along with the corresponding total number of available beds. Sort records by the total available beds in descending order.

🔍 It's straightforward, read the question carefully and give it a try! 👇

𝐒𝐜𝐡𝐞𝐦𝐚 𝐚𝐧𝐝 𝐃𝐚𝐭𝐚𝐬𝐞𝐭:
CREATE TABLE airbnb_apartments(host_id int,apartment_id varchar(5),apartment_type varchar(10),n_beds int,n_bedrooms int,country varchar(20),city varchar(20));
INSERT INTO airbnb_apartments VALUES(0,'A1','Room',1,1,'USA','NewYork'),(0,'A2','Room',1,1,'USA','NewJersey'),(0,'A3','Room',1,1,'USA','NewJersey'),(1,'A4','Apartment',2,1,'USA','Houston'),(1,'A5','Apartment',2,1,'USA','LasVegas'),(3,'A7','Penthouse',3,3,'China','Tianjin'),(3,'A8','Penthouse',5,5,'China','Beijing'),(4,'A9','Apartment',2,1,'Mali','Bamako'),(5,'A10','Room',3,1,'Mali','Segou')

CREATE TABLE airbnb_hosts(host_id int,nationality  varchar(15),gender varchar(5),age int);
INSERT INTO airbnb_hosts  VALUES(0,'USA','M',28),(1,'USA','F',29),(2,'China','F',31),(3,'China','M',24),(4,'Mali','M',30),(5,'Mali','F',30);
-------------

Solution :

                                select
                                    ah.nationality as Nationality,
                                    sum(n_beds) as No_Of_Beds
                                from
                                    airbnb_hosts ah

                                inner join

                                    airbnb_apartments aa

                                on ah.host_id = aa.host_id

                                group by ah.nationality

                                order by No_Of_Beds DESC;
"""



from pyspark.sql import SparkSession
from pyspark.sql.functions import col,lit,sum
import logging

logging.basicConfig(level=logging.INFO,format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

if __name__=='__main__':

    logger.info("Start Application")

    spark = SparkSession.builder.appName("Available_Beds_Per_Hosts")\
            .config("spark.ui.port","4041")\
            .config("spark.jars","file:///C://Users/shree/Downloads/postgresql-42.7.4.jar")\
            .master("local[*]")\
            .getOrCreate()


    jdbc_url = "jdbc:postgresql://localhost:5432/postgres"

    query = "(Select * from airbnb_apartments) As Query"

    connection_properties = {

        "user" : "postgres",
        "password": "Pkts1t4j11@",
        "driver" : "org.postgresql.Driver"
    }


    airbnb_apartments = spark.read.jdbc(url=jdbc_url,table=query,properties=connection_properties)

    logger.info("Table : AirBnb apartments")

    airbnb_apartments.show(truncate=False)

    query = "(select * from airbnb_hosts) AS Query"

    airbnb_hosts = spark.read.jdbc(url = jdbc_url,table=query,properties=connection_properties)

    logger.info("Table : AirBnb hosts")

    airbnb_hosts.show(truncate=False)


    joined_df = airbnb_hosts.alias("ah")\
        .join(
                    airbnb_apartments.alias("aa"),
                    col("ah.host_id") == col("aa.host_id"),
                    "inner"
        )

    logger.info("JOINED DATAFRAME")

    joined_df.show(truncate=False)

    final_df = joined_df.groupBy(col("ah.nationality").alias("Nationality"))\
                        .agg(sum(col("aa.n_beds")).alias("No_Of_Beds"))

    final_df = final_df.select(col("Nationality"),col("No_Of_Beds"))

    logger.info("Final Result : ")

    final_df.show(truncate=False)

    final_df.repartition(1).write\
    .option("header",True)\
    .mode("overwrite")\
    .format("csv")\
    .save("file:///C://Users/shree/PycharmProjects/TransformationsFromQueries/output-csv-available-beds-per-hosts")






    logger.info("End Application")
