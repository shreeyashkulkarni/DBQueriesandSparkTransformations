from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StringType,IntegerType,StructField
from pyspark.sql.functions import col,lit,when,size,collect_set
import logging
logging.basicConfig(level=logging.INFO,format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)



if __name__ == '__main__':

    logger.info("start_application")

    # os.environ["PYSPARK_PYTHON"] = "python"

    try :
        spark = SparkSession.builder.appName("Shree Spark-App")\
                .config("spark.ui.port","4041")\
                .master("local[*]")\
                .getOrCreate()

        spark.sparkContext.setLogLevel("WARN")
        logger.info("Spark Session Create Successfully")

    except:
        logger.error("Failed to create Sparksession",exec_info=True)
        raise





    dfdata =        [
                        ("John", 1234, 9876, "Savings"),
                        ("John", 1234, 9875, "Current"),
                        ("Marry", 1000, 8902, "Current"),
                        ("Tom", 2000, 3452, "Savings")
                    ]


    dfschema = StructType([

                            StructField("Name",StringType(),nullable=False),
                            StructField("Customer_Id",IntegerType(),nullable=False),
                            StructField("Account_Number",IntegerType(),nullable=False),
                            StructField("Account_Type",StringType(),nullable=False)

    ])


    df = spark.createDataFrame(data = dfdata,schema = dfschema)

    logger.info("Original Data Frame")
    df.show(truncate=False)


    aggregated_df = df.groupBy(col("Name"),col("Customer_Id"))\
                        .agg((collect_set(col("Account_Type"))).alias("AccountTypes"))
    aggregated_df.show(truncate=False)


    finaldf = aggregated_df.withColumn(
        "Final_Account_type",
        when(size(col("AccountTypes")) > 1,"Mixed").otherwise(col("AccountTypes")[0])
    )

    finaldf.show(truncate=False)






    logger.info("stop application")

