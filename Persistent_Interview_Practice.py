
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession,Row
from pyspark.sql.window import Window
import logging


logging.basicConfig(level=logging.INFO,format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


if __name__=='__main__':

    logger.info("Spark Application Start")

    try:
        spark = SparkSession.builder.appName("Spark-App")\
                .config("spark.ui.port","4041")\
                .config("spark.jars","file:///C://Users/shree/Downloads/postgresql-42.7.4.jar")\
                .master("local[*]")\
                .getOrCreate()

        spark.sparkContext.setLogLevel("WARN")

    except Exception as e:
        logger.error("Failed to Create Spark Session",exec_info = True)
        raise



    jdbc_url = "jdbc:postgresql://localhost:5432/postgres"
    query = "(Select * from employee_new) AS QUERY"
    connection_properties = {

        "user":"postgres",
        "password" : "Pkts1t4j11@",
        "driver" : "org.postgresql.Driver"
    }



    df = spark.read.jdbc(
                            url = jdbc_url,
                            table=query,
                            properties=connection_properties
    )



    df.show(truncate=False)

    windowspecs = Window.partitionBy(col("id")).orderBy(col("salary"))

    """
    Removing Duplicates
    """


    numberingdatadf = df.withColumn("Row_Number",row_number().over(windowspecs))

    numberingdatadf.show(truncate=False)


    finaldf = numberingdatadf.filter(col("Row_Number") == 1).drop(col("Row_Number"))
    finaldf.show(truncate=False)



    """
    Second Highest Salary
    """

    query = "(Select * from employee_information_table) AS QUERY"

    employeeinformationtabledf = spark.read.jdbc(
                                                    url=jdbc_url,
                                                    table=query,
                                                    properties=connection_properties
    )

    employeeinformationtabledf.show(truncate=False)

    peoplewhojoinedlastyeardf = employeeinformationtabledf\
                            .filter(year(col("empjoiningdata")) == (year(current_date()) - 1))
    peoplewhojoinedlastyeardf.show(truncate=False)

    windowspecs2 = Window.orderBy(col("empsalary").desc())

    NumberingSalarydf = peoplewhojoinedlastyeardf.withColumn("RN",row_number().over(windowspecs2))
    NumberingSalarydf.show(truncate=False)

    secondhighestsalary = NumberingSalarydf.filter(col("RN") == 2)
    secondhighestsalary.show(truncate=False)


    logger.info("Spark Application End")







