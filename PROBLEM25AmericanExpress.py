"""
𝐌𝐮𝐬𝐭 𝐓𝐫𝐲: American Express (Medium Level) #SQL Interview Question — Solution

American Express is reviewing their customers' transactions, and you have been tasked with locating the customer who has the third highest total transaction amount. The output should include the customer's id, as well as their first name and last name. For ranking the customers, use type of ranking with no gaps between subsequent ranks.

🔍By solving this, you'll learn how to use mutiple join. Give it a try and share the output! 👇

𝐒𝐜𝐡𝐞𝐦𝐚 𝐚𝐧𝐝 𝐃𝐚𝐭𝐚𝐬𝐞𝐭:
CREATE TABLE customers (id INT,first_name VARCHAR(50),last_name VARCHAR(50),city VARCHAR(100),address VARCHAR(200),phone_number VARCHAR(20));

INSERT INTO customers (id, first_name, last_name, city, address, phone_number) VALUES(1, 'Jill', 'Doe', 'New York', '123 Main St', '555-1234'),(2, 'Henry', 'Smith', 'Los Angeles', '456 Oak Ave', '555-5678'),(3, 'William', 'Johnson', 'Chicago', '789 Pine Rd', '555-8765'),(4, 'Emma', 'Daniel', 'Houston', '321 Maple Dr', '555-4321'),(5, 'Charlie', 'Davis', 'Phoenix', '654 Elm St', '555-6789');

CREATE TABLE card_orders (order_id INT,cust_id INT,order_date DATETIME,order_details VARCHAR(255),total_order_cost INT);

INSERT INTO card_orders (order_id, cust_id, order_date, order_details, total_order_cost) VALUES(1, 1, '2024-11-01 10:00:00', 'Electronics', 200),(2, 2, '2024-11-02 11:30:00', 'Groceries', 150),(3, 1, '2024-11-03 15:45:00', 'Clothing', 120),(4, 3, '2024-11-04 09:10:00', 'Books', 90),(8, 3, '2024-11-08 10:20:00', 'Groceries', 130),(9, 1, '2024-11-09 12:00:00', 'Books', 180),(10, 4, '2024-11-10 11:15:00', 'Electronics', 200),(11, 5, '2024-11-11 14:45:00', 'Furniture', 150),(12, 2, '2024-11-12 09:30:00', 'Furniture', 180);
-----------

Solution : 
                WITH customer_spending AS(
								SELECT 
										 c.id,
								         c.first_name,
								         c.last_name,
								         SUM(co.total_order_cost) AS total_sum
								
								
								FROM
										customers as c
										JOIN
										card_orders as co
								ON 
										c.id = co.cust_id
								group by c.id,c.first_name,c.last_name
                                ),
                                ranked_customers AS (
                                                        SELECT 
                                                                *,
                                                                DENSE_RANK() OVER(ORDER BY total_sum DESC) AS rn
                                                        FROM customer_spending
                                )
                                SELECT
                                    id,
                                    first_name,
                                    last_name,
                                    total_sum
                                FROM ranked_customers
                                WHERE rn = 3;
"""
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col,lit,when,sum,dense_rank
import logging


logging.basicConfig(level=logging.INFO,format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)



if __name__ == "__main__":

    logger.info("Starting Spark Application")


    try:
        spark = SparkSession.builder.appName("Spark-Problem25AmericanExpress")\
                                     .config("spark.ui.port","4041")\
                                     .config("spark.jars","file:///C://Users/shree/Downloads/postgresql-42.7.4.jar")\
                                     .master("local[*]")\
                                     .getOrCreate()
        
        spark.sparkContext.setLogLevel("WARN")

    except Exception as e:
        logger.error("Failed to create Spark Session",exec_info=True)
        raise


    jdbc_url = "jdbc:postgresql://localhost:5432/postgres"
    query = "(select * from customers) AS query"
    connection_properties = {
            "user" : "postgres",
            "password" : "Pkts1t4j11@",
            "driver": "org.postgresql.Driver"
    }


    customers_df = spark.read.jdbc(url = jdbc_url,table = query , properties = connection_properties)
    customers_df.show(truncate=False)

    query = "(select * from card_orders) AS query"

    card_orders_df = spark.read.jdbc(url = jdbc_url,table = query , properties = connection_properties)
    card_orders_df.show(truncate=False)



    car_spending_df = customers_df.alias("c").join(card_orders_df.alias("co"),col("c.id") == col("co.cust_id")).groupBy(col("c.id"),col("c.first_name"),col("c.last_name"))\
                                             .agg(sum(col("co.total_order_cost")).alias("total_sum"))
    
    car_spending_df.show(truncate=False)


    window_spec = Window.orderBy(col("total_sum").desc())

    result_df = car_spending_df.withColumn("rnk",dense_rank().over(window_spec))
    result_df.show(truncate=False)

    final_df = result_df.filter(col("rnk") == 3).select("id","first_name","last_name")
    final_df.show(truncate=False)




    logger.info("Ending Spark Application")

