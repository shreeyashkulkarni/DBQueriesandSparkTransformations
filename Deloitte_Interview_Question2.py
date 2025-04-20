from pyspark.sql import SparkSession
from pyspark.sql.functions import col,lit,split,when,size
import logging

logging.basicConfig(level=logging.INFO,format = '%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


if __name__ == '__main__':

    logger.info("Start Application")

    try :
        spark = SparkSession.builder.appName("Shree Spark-App")\
                .config("spark.ui.port","4041")\
                .master("local[*]")\
                .getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        logger.info("SparkSession created successfully")

    except Exception as e:
        logger.error("Failed to create Spark Session",exec_info=True)
        raise

    data = [("Rohit|Kumar|Gupta",), ("Rohit|Gupta",)]
    columns = ["Full_Name"]

    df = spark.createDataFrame(data,columns)
    df.show()



    df = df.withColumn("name_parts",split(col("Full_Name"),"\|"))
    df.show(truncate=False)




    finaldf = df.withColumn("First_Name",col('name_parts').getItem(0))\
                .withColumn("Middle_Name",when(size(col("name_parts")) == 3,col("name_parts").getItem(1)).otherwise(""))\
                .withColumn("Last_Name",when(size(col("name_parts")) == 3,col("name_parts").getItem(2))\
                                                .when(size(col("name_parts")) == 2,col("name_parts").getItem(1))\
                                                .otherwise(""))

    finaldf.show(truncate=False)




    logger.info("End Application")
