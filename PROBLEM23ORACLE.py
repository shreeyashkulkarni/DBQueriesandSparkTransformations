"""
𝐌𝐮𝐬𝐭 𝐓𝐫𝐲: Oracle(Hard Level) #SQL Interview Question — Solution

Write a query that compares each employee's salary to their manager's and the average department salary (excluding the manager's salary). Display the department, employee ID, employee's salary, manager's salary, and department average salary. Order by department, then by employee salary (highest to lowest).

🔍By solving this, you'll learn how to use cte, mutiple join, groupby. Give it a try and share the output! 👇

𝐒𝐜𝐡𝐞𝐦𝐚 𝐚𝐧𝐝 𝐃𝐚𝐭𝐚𝐬𝐞𝐭:
CREATE TABLE employee_o (id INT PRIMARY KEY,first_name VARCHAR(50),last_name VARCHAR(50),age INT,gender VARCHAR(10),employee_title VARCHAR(50),department VARCHAR(50),salary INT,manager_id INT);

INSERT INTO employee_o (id, first_name, last_name, age, gender, employee_title, department, salary, manager_id) VALUES(1, 'Alice', 'Smith', 45, 'F', 'Manager', 'HR', 9000, 1),(2, 'Bob', 'Johnson', 34, 'M', 'Assistant', 'HR', 4500, 1),(3, 'Charlie', 'Williams', 28, 'M', 'Coordinator', 'HR', 4800, 1),(4, 'Diana', 'Brown', 32, 'F', 'Manager', 'IT', 12000, 4),(5, 'Eve', 'Jones', 27, 'F', 'Analyst', 'IT', 7000, 4),(6, 'Frank', 'Garcia', 29, 'M', 'Developer', 'IT', 7500, 4),(7, 'Grace', 'Miller', 30, 'F', 'Manager', 'Finance', 10000, 7),(8, 'Hank', 'Davis', 26, 'M', 'Analyst', 'Finance', 6200, 7),(9, 'Ivy', 'Martinez', 31, 'F', 'Clerk', 'Finance', 5900, 7),(10, 'John', 'Lopez', 36, 'M', 'Manager', 'Marketing', 11000, 10),(11, 'Kim', 'Gonzales', 29, 'F', 'Specialist', 'Marketing', 6800, 10),(12, 'Leo', 'Wilson', 27, 'M', 'Coordinator', 'Marketing', 6600, 10);


Solution : 
            WITH avg_dept_sal AS(
						SELECT
								department,
								ROUND(
										AVG(
												CASE
													WHEN employee_title <> 'Manager'
													THEN salary
												END
										)
								) as avg_department_salary
						FROM
								employee_o
						GROUP BY department
)
SELECT
		e1.id,
		e1.department,
		e1.salary AS employee_salary,
		CASE 
			WHEN e1.id = e2.id THEN NULL
			ELSE e2.salary
		END AS manager_salary,
		ads.avg_department_salary
FROM
		employee_o as e1
		JOIN
		employee_o as e2
ON 
		e1.manager_id = e2.id

JOIN 
		avg_dept_sal AS ads
ON 
		e1.department = ads.department
ORDER BY e1.department,employee_salary;


-----------

"""


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit,avg,when,round
import logging

logging.basicConfig(level = logging.INFO,format = "%(asctime)s - %(levelname)s -%(message)s")
logger = logging.getLogger(__name__)



if __name__ == "__main__":
    
	logger.info("Starting Spark Application")

	try:
		spark = SparkSession.builder.appName("Spark-Problem23")\
				.config("spark.ui.port","4041")\
				.config("spark.jars","file:///C://Users/shree/Downloads/postgresql-42.7.4.jar")\
				.master("local[*]")\
				.getOrCreate()
	
		spark.sparkContext.setLogLevel("WARN")

	except Exception as e:
		logger.error("Failed to create spark session",exec_info=True)
		raise


	

	jdbc_url = "jdbc:postgresql://localhost:5432/postgres"
	query = "(select * from employee_o) AS query"
	connection_properties = {
		"user":"postgres",
		"password":"Pkts1t4j11@",
		"driver":"org.postgresql.Driver"
	}

	employees_df = spark.read.jdbc(url = jdbc_url,table=query,properties= connection_properties)
	employees_df.show(truncate=False)


	avg_dept_sal_df = employees_df.groupBy("department").agg(
																round(avg(
																	when((col("employee_title") != "Manager"),col("salary")
																	
																	))).alias("Average_Department_Salary")
	)
	avg_dept_sal_df.show(truncate=False)


	aggregated_self_join = employees_df.alias("e1").join(employees_df.alias("e2"),\
													  col("e1.manager_id") == col("e2.id"))\
													  .select(col("e1.id")\
					   										 ,col("e1.department")\
					 										,col("e1.salary").alias("employee_salary")\
															,when( col("e1.id") == col("e2.id")\
					 														,lit(None))\
															.otherwise(col("e2.salary")).alias("manager_salary")
															).alias("subquery")\
													.join(avg_dept_sal_df.alias("ads"),col("subquery.department") == col("ads.department"))
													
	aggregated_self_join.show(truncate=False)				

	aggregated_self_join = aggregated_self_join.orderBy(
															col("subquery.department")\
														   ,col("employee_salary")
																)		   
	aggregated_self_join.show(truncate=False)


	logger.info("Ending Spark Application")

