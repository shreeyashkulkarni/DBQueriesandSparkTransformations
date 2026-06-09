"""
𝐌𝐮𝐬𝐭 𝐓𝐫𝐲: Walmart, Paypal (Medium Level) #SQL Interview Question — Solution

Find managers with at least 7 direct reporting employees. In situations where user is reporting to himself/herself, count that also.
Output first names of managers.

🔍By solving this, you'll learn how to use self join. Give it a try and share the output! 👇

𝐒𝐜𝐡𝐞𝐦𝐚 𝐚𝐧𝐝 𝐃𝐚𝐭𝐚𝐬𝐞𝐭:
CREATE TABLE employees (id INT PRIMARY KEY,first_name VARCHAR(50),last_name VARCHAR(50),age INT,sex VARCHAR(10),employee_title VARCHAR(50),department VARCHAR(50),salary INT,target INT,bonus INT,email VARCHAR(100),city VARCHAR(50),address VARCHAR(255),manager_id INT);

INSERT INTO employees (id, first_name, last_name, age, sex, employee_title, department, salary, target, bonus, email, city, address, manager_id) VALUES(1, 'Alice', 'Smith', 40, 'F', 'Manager', 'Sales', 90000, 100000, 15000, 'alice.smith@example.com', 'New York', '123 Main St', 1),(2, 'Bob', 'Johnson', 35, 'M', 'Team Lead', 'Sales', 80000, 95000, 12000, 'bob.johnson@example.com', 'Chicago', '456 Oak St', 1),(3, 'Carol', 'Williams', 30, 'F', 'Sales Executive', 'Sales', 70000, 85000, 10000, 'carol.williams@example.com', 'New York', '789 Pine St', 1),(4, 'David', 'Brown', 28, 'M', 'Sales Executive', 'Sales', 68000, 80000, 9000, 'david.brown@example.com', 'Chicago', '101 Maple St', 1),(5, 'Emma', 'Jones', 32, 'F', 'Sales Executive', 'Sales', 71000, 86000, 9500, 'emma.jones@example.com', 'New York', '202 Cedar St', 1),(6, 'Frank', 'Miller', 45, 'M', 'Manager', 'Engineering', 95000, 105000, 16000, 'frank.miller@example.com', 'San Francisco', '303 Spruce St', 6),(7, 'Grace', 'Davis', 29, 'F', 'Engineer', 'Engineering', 73000, 87000, 11000, 'grace.davis@example.com', 'San Francisco', '404 Willow St', 6);
-----------

Solution : 
            select e1.first_name from employees as e1 join employees as e2 on e1.id = e2.manager_id group by e1.first_name having count(e1.id) >=7 ;


"""




from pyspark.sql import SparkSession
from pyspark.sql.functions import col,count
import logging

logging.basicConfig(level=logging.INFO,format = "%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


if __name__ == "__main__":
    logger.info("Start Spark Application")



    try:


        spark = SparkSession.builder.appName("spark-PROBLEM22WALMARTPAYPAL")\
                .config("spark.ui.port","4041")\
                .config("spark.jars","file:///C://Users/shree/Downloads/postgresql-42.7.4.jar")\
                .master("local[*]")\
                .getOrCreate()
        

        spark.sparkContext.setLogLevel("WARN")

    except Exception as e :
        logger.error("Failed to create Spark Session",exec_info = True)
        raise




    jdbc_url = "jdbc:postgresql://localhost:5432/postgres"
    query = "(select * from employees) AS query"
    connection_properties = {
        "user":"postgres",
        "password":"Pkts1t4j11@",
        "driver":"org.postgresql.Driver"
    }

    employees_df = spark.read.jdbc(

                url = jdbc_url,
                table = query,
                properties = connection_properties

    )


    joined_df = employees_df.alias("e1").join(employees_df.alias("e2"),col("e1.id") == col("e2.manager_id"))
    joined_df.show(truncate=False)

    aggregated_df = joined_df.groupBy(col("e1.first_name")).agg(
        count("e1.first_name")
    )
    aggregated_df.show(truncate=False)

    final_df = aggregated_df.where(col("first_name") >= 7)

    final_df.show(truncate=False)


    logger.info("End Spark Application")




