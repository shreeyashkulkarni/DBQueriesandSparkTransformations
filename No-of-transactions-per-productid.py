"""

𝐌𝐮𝐬𝐭 𝐓𝐫𝐲: Nvidia, Microsoft (Medium Level) hashtag#SQL Interview Question — Solution

Find the number of transactions that occurred for each product. Output the product name along with the corresponding number of transactions and order records by the product id in ascending order. You can ignore products without transactions.

🔍 By solving this, you'll learn how to use join with grouping. Give it a try and share the output! 👇

𝐒𝐜𝐡𝐞𝐦𝐚 𝐚𝐧𝐝 𝐃𝐚𝐭𝐚𝐬𝐞𝐭:
CREATE TABLE excel_sql_inventory_data (product_id INT,product_name VARCHAR(50),product_type VARCHAR(50),unit VARCHAR(20),price_unit FLOAT,wholesale FLOAT,current_inventory INT);

INSERT INTO excel_sql_inventory_data (product_id, product_name, product_type, unit, price_unit, wholesale, current_inventory)
VALUES(1, 'strawberry', 'produce', 'lb', 3.28, 1.77, 13),(2, 'apple_fuji', 'produce', 'lb', 1.44, 0.43, 2),(3, 'orange', 'produce', 'lb', 1.02, 0.37, 2),(4, 'clementines', 'produce', 'lb', 1.19, 0.44, 44),(5, 'blood_orange', 'produce', 'lb', 3.86, 1.66, 19);

CREATE TABLE excel_sql_transaction_data (transaction_id INT PRIMARY KEY,time DATETIME,product_id INT);

INSERT INTO excel_sql_transaction_data (transaction_id, time, product_id)
VALUES(153, '2016-01-06 08:57:52', 1),(91, '2016-01-07 12:17:27', 1),(31, '2016-01-05 13:19:25', 1),(24, '2016-01-03 10:47:44', 3),(4, '2016-01-06 17:57:42', 3),(163, '2016-01-03 10:11:22', 3),(92, '2016-01-08 12:03:20', 2),(32, '2016-01-04 19:37:14', 4),(253, '2016-01-06 14:15:20', 5),(118, '2016-01-06 14:27:33', 5);
------------


Solution :

                                    SELECT


                                            idata.product_name
                                            COUNT(tdata.transaction_id) as No_of_Transactions

                                    FROM

                                            excel_sql_inventory_data as idata

                                    inner join

                                            excel_sql_transaction_data as tdata

                                    on

                                            idata.product_id = tdata.product_id

                                    GROUP BY

                                            idata.product_id , idata.product_name

                                    ORDER BY

                                            idata.product_id

                                    asc



"""



from pyspark.sql import SparkSession
from pyspark.sql.functions import col,count
from pyspark.sql.types import StructType,StructField,StringType,IntegerType
import logging


logging.basicConfig(level=logging.INFO,format="%(asctime)s - %(levelname)s - %(message)s ")
logger = logging.getLogger(__name__)



if __name__=="__main__":

    logger.info("Start Spark Application")

    try:
        spark : SparkSession =  SparkSession.builder.appName("Shree-Spark-App")\
                                .config("spark.ui.port","4041")\
                                .config("spark.jars","file:///C://Users/shree/Downloads/postgresql-42.7.4.jar")\
                                .master("local[*]")\
                                .getOrCreate()

        spark.sparkContext.setLogLevel("WARN")

        logger.info("Spark Session Created Successfully")

        jdbcUrl = "jdbc:postgresql://localhost:5432/postgres"
        jdbcQuery = "(Select * from excel_sql_inventory_data) AS QUERY"
        jdbcUsername = "postgres"
        jdbcPassword = "Pkts1t4j11@"
        jdbcDriver = "org.postgresql.Driver"

        connection_properties = {

            "user": jdbcUsername,
            "password": jdbcPassword,
            "driver": jdbcDriver
        }

        logger.info("TABLE : excel_sql_inventory_data")

        excel_sql_inventory_datadf = spark.read.jdbc(url=jdbcUrl, table=jdbcQuery, properties=connection_properties)

        excel_sql_inventory_datadf.printSchema()

        excel_sql_inventory_datadf.show(truncate=False)


        logger.info("TABLE : excel_sql_transaction_data")

        jdbcQuery = "(Select * from excel_sql_transaction_data) AS QUERY"

        excel_sql_transaction_datadf = spark.read\
                                        .format("jdbc")\
                                        .option("url",jdbcUrl)\
                                        .option("user",jdbcUsername)\
                                        .option("password",jdbcPassword)\
                                        .option("driver","org.postgresql.Driver")\
                                        .option("dbTable",jdbcQuery)\
                                        .load()

        excel_sql_transaction_datadf.printSchema()

        excel_sql_transaction_datadf.show(truncate = False)


        logger.info("INNER JOIN on excel_sql_inventory_data and excel_sql_transaction_data")

        joineddf =  excel_sql_inventory_datadf.alias("idata")\
                                .join(
                                        excel_sql_transaction_datadf.alias("tdata"),
                                        col("idata.product_id") == col("tdata.product_id"),
                                        "inner"
                                )

        joineddf.show(truncate = False)


        groupeddf = joineddf.groupBy(
                                        col("idata.product_id"),
                                        col("idata.product_name")
        )\
            .agg(
                    count(col("tdata.transaction_id")).alias("No_of_Transactions")
              )\
            .select(
                        col("idata.product_name"),
                        col("No_of_Transactions")
                    )




        logger.info("FINAL DATAFRAME")

        finaldf = groupeddf.orderBy(col("idata.product_id").asc())

        finaldf.show(truncate = False)

        finaldf.repartition(1).write\
            .option("header",True)\
            .option("mode","overwrite")\
            .format("csv") \
            .save("file:///C://Users/shree/PycharmProjects/TransformationsFromQueries/output-csv-no-of-transacations")











    except Exception as e:
        logger.error("Failed to create Spark Session", exec_info = True)
        raise










    logger.info("End Spark Application")









