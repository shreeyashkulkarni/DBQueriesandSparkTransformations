"""
Write a query that calculates the difference between the highest salaries found in the marketing and engineering departments. Output just the absolute difference in salaries.

🔍 By solving this, you'll learn how to use case and join. Give it a try later and share the output! 👇

𝐒𝐜𝐡𝐞𝐦𝐚 𝐚𝐧𝐝 𝐃𝐚𝐭𝐚𝐬𝐞𝐭:
CREATE TABLE db_employee (id INT,first_name VARCHAR(50),last_name VARCHAR(50),salary INT,department_id INT);

INSERT INTO db_employee (id, first_name, last_name, salary, department_id) VALUES(10306, 'Ashley', 'Li', 28516, 4),(10307, 'Joseph', 'Solomon', 19945, 1),(10311, 'Melissa', 'Holmes', 33575, 1),(10316, 'Beth', 'Torres', 34902, 1),(10317, 'Pamela', 'Rodriguez', 48187, 4),(10320, 'Gregory', 'Cook', 22681, 4),(10324, 'William', 'Brewer', 15947, 1),(10329, 'Christopher', 'Ramos', 37710, 4),(10333, 'Jennifer', 'Blankenship', 13433, 4),(10339, 'Robert', 'Mills', 13188, 1);

CREATE TABLE db_dept (id INT,department VARCHAR(50));

INSERT INTO db_dept (id, department) VALUES(1, 'engineering'),(2, 'human resource'),(3, 'operation'),(4, 'marketing');



Solution :
            		select
                            ABS(
                                    MAX(CASE WHEN d.department = 'marketing' THEN e.salary END) -
                                    MAX(CASE WHEN d.department = 'engineering' THEN e.salary END)

                            ) AS salary_difference


		            from
			                db_employee as dbe
		            inner join
			                db_dept as dbdp
		            on
			                dbe.department_id  = dbdp.id

"""


from pyspark.sql import SparkSession,Row,DataFrame
from pyspark.sql.functions import col,max,abs,when
from pyspark.sql.types import StructType,StructField
import logging

logging.basicConfig(level = logging.INFO,format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


if __name__ == '__main__':

    logger.info("Spark Application Start")


    try :

        spark = SparkSession.builder.appName("Spark-App")\
                .config("spark.ui.port","4041")\
                .config("spark.driver.memory","12g")\
                .config("spark.executor.memory","10g")\
                .config("spark.executor.instances","4")\
                .config("spark.jars","file:///C://Users/shree/Downloads/postgresql-42.7.4.jar")\
                .master("local[*]")\
                .getOrCreate()

        logger.info("Spark Session created Successfully")

        spark.sparkContext.setLogLevel("WARN")








    except Exception as e:
        logger.error("Failed to create Spark Session",exec_info=True)
        raise



    db_employeedf = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/postgres") \
        .option("user", "postgres") \
        .option("password", "Pkts1t4j11@") \
        .option("dbTable", "(Select * from db_employee) As Query") \
        .option("driver", "org.postgresql.Driver") \
        .load()

    logger.info("Table : db_employee")


    db_employeedf.show(truncate=False)


    db_deptdf = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/postgres") \
        .option("user", "postgres") \
        .option("password", "Pkts1t4j11@") \
        .option("dbTable", "(Select * from db_dept) As Query") \
        .option("driver", "org.postgresql.Driver") \
        .load()

    logger.info("Table : db_dept")

    db_deptdf.show(truncate=False)



    joineddf = db_employeedf.alias("dbe").join(
                                                    db_deptdf.alias("dbdp"),
                                                    col("dbe.department_id") == col("dbdp.id"),
                                                    "inner"
                                                )

    logger.info("Inner Join on db_employee and db_dept")

    joineddf.show(truncate=False)


    finaldf = joineddf.agg(
                                max(when(col("dbdp.department") == "marketing" , col("dbe.salary"))).alias("marketing_max"),
                                max(when(col("dbdp.department") == "engineering",col("dbe.salary"))).alias("engineering_max")

    )\
    .withColumn("abs_diff",abs(col("marketing_max") - col("engineering_max")))

    logger.info("Difference between max marketing and max engineering ")

    finaldf.show(truncate = False)








    logger.info("Spark Application End")