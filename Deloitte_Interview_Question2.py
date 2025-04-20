from pyspark.sql import SparkSession
from pyspark.sql.functions import col,lit,split
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

    






    logger.info("End Application")
