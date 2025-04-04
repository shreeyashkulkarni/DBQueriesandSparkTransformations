"""

𝐌𝐮𝐬𝐭 𝐓𝐫𝐲: Microsoft(Medium Level) hashtag#SQL Interview Question — Solution

Given a list of projects and employees mapped to each project,
calculate by the amount of project budget allocated to each employee.
The output should include the project title and the project budget rounded to the closest integer.
Order your list by projects with the highest budget per employee first.

⛳It's straightforward only but bit twisted, read the question carefully and give it a try! 👇

𝐒𝐜𝐡𝐞𝐦𝐚 𝐚𝐧𝐝 𝐃𝐚𝐭𝐚𝐬𝐞𝐭:
CREATE TABLE ms_projects(id int, title varchar(15), budget int);
INSERT INTO ms_projects VALUES (1, 'Project1',  29498),(2, 'Project2',  32487),(3, 'Project3',  43909),(4, 'Project4',  15776),(5, 'Project5',  36268),(6, 'Project6',  41611),(7, 'Project7',  34003),(8, 'Project8',  49284),(9, 'Project9',  32341),(10, 'Project10',    47587),(11, 'Project11',    11705),(12, 'Project12',    10468),(13, 'Project13',    43238),(14, 'Project14',    30014),(15, 'Project15',    48116),(16, 'Project16',    19922),(17, 'Project17',    19061),(18, 'Project18',    10302),(19, 'Project19',    44986),(20, 'Project20',    19497);

CREATE TABLE ms_emp_projects(emp_id int, project_id int);
INSERT INTO ms_emp_projects VALUES (10592,  1),(10593,  2),(10594,  3),(10595,  4),(10596,  5),(10597,  6),(10598,  7),(10599,  8),(10600,  9),(10601,  10),(10602, 11),(10603, 12),(10604, 13),(10605, 14),(10606, 15),(10607, 16),(10608, 17),(10609, 18),(10610, 19),(10611, 20);
--------------

Solution :


                                select
                                        mp.title as project_title,
                                        ROUND(mp.budget * 1.0 / count(mep.emp_id)) As budget_per_employee
                                from
                                    ms_projects as mp
                                inner join
                                    ms_emp_projects as mep
                                on
                                    mp.id  = mep.project_id
                                group by
                                    mp.id,
                                    mp.title ,
                                    mp.budget
                                order by
                                    budget_per_employee
                                desc



"""

from pyspark.sql import SparkSession
import logging
from pyspark.sql.functions import col,lit,count

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

        logger.info("Successfully Created Spark Session")

    except Exception as e:
        logger.error("Failed to create Spark Session")
        raise


    jdbc_url = "jdbc:postgresql://localhost:5432/postgres"
    query = "(select * from ms_projects) As Query"
    connection_properties = {

        "user":"postgres",
        "password":"Pkts1t4j11@",
        "driver":"org.postgresql.Driver"

    }



    ms_projectsdf = spark.read.jdbc(
        url = jdbc_url,
        table = query,
        properties = connection_properties
    )

    logger.info("TABLE : ms_projects")
    ms_projectsdf.show(truncate=False)

    query = "(select * from ms_emp_projects) As query"

    ms_emp_projectsdf = spark.read.jdbc(

                url = jdbc_url,
                table = query,
                properties=connection_properties
    )

    logger.info("TABLE : ms_emp_projects")
    ms_emp_projectsdf.show(truncate=False)



    joined_df  = ms_projectsdf.alias("mp")\
                 .join(ms_emp_projectsdf.alias("mep"),col("mp.id") == col("mep.project_id"),"inner")


    finaldf = joined_df.groupBy(col("mp.id"),col("mp.title"),col("mp.budget"))\
                         .agg((col("mp.budget")/count("mep.emp_id")).alias("budget_per_employee"))\
                         .orderBy(col("budget_per_employee").desc())\
                         .select(col("mp.title").alias("Project_title"),col("Budget_Per_Employee"))


    logger.info("Final Data Frame")

    finaldf.show(truncate=False)

    logger.info("Saving File : file:///C://Users/shree/PycharmProjects/TransformationsFromQueries/output-csv-budget-allocation")

    finaldf.repartition(1).write\
           .option("header",True)\
           .option("InferSchema",True)\
           .mode("overwrite")\
           .format("csv")\
           .save("file:///C://Users/shree/PycharmProjects/TransformationsFromQueries/output-csv-budget-allocation/")




    logger.info("Spark Application End")
