"""
𝐌𝐮𝐬𝐭 𝐓𝐫𝐲: Amazon (Hard Level) #SQL Interview Question — Solution

Find products which are exclusive to only Amazon and therefore not sold at Top Shop and Macy's. Your output should include the product name, brand name, price, and rating.

Two products are considered equal if they have the same product name and same maximum retail price (mrp column).

🔍By solving this, you'll learn how to use mutiple join. Give it a try and share the output! 👇

𝐒𝐜𝐡𝐞𝐦𝐚 𝐚𝐧𝐝 𝐃𝐚𝐭𝐚𝐬𝐞𝐭:
CREATE TABLE innerwear_amazon_com (product_name VARCHAR(255),mrp VARCHAR(50),price VARCHAR(50),pdp_url VARCHAR(255),brand_name VARCHAR(100),product_category VARCHAR(100),retailer VARCHAR(100),description VARCHAR(255),rating FLOAT,review_count INT,style_attributes VARCHAR(255),total_sizes VARCHAR(50),available_size VARCHAR(50),color VARCHAR(50));

CREATE TABLE innerwear_macys_com (product_name VARCHAR(255),mrp VARCHAR(50),price VARCHAR(50),pdp_url VARCHAR(255),brand_name VARCHAR(100),product_category VARCHAR(100),retailer VARCHAR(100),description VARCHAR(255),rating FLOAT,review_count FLOAT,style_attributes VARCHAR(255),total_sizes VARCHAR(50),available_size VARCHAR(50),color VARCHAR(50));

CREATE TABLE innerwear_topshop_com (product_name VARCHAR(255),mrp VARCHAR(50),price VARCHAR(50),pdp_url VARCHAR(255),brand_name VARCHAR(100),product_category VARCHAR(100),retailer VARCHAR(100),description VARCHAR(255),rating FLOAT,review_count FLOAT,style_attributes VARCHAR(255),total_sizes VARCHAR(50),available_size VARCHAR(50),color VARCHAR(50));


Solution : 

        My solution :

        with amazon_exclusive_macy AS(
									SELECT 
											am.product_name as amazon_product_name,
											am.mrp,
											macy.product_name, 
											macy.mrp
									FROM
											innerwear_amazon_com am
											JOIN
											innerwear_macys_com macy
									ON
											am.product_name =macy.product_name 
											AND
											am.mrp = macy.mrp
                    ),
                    amazon_exclusive_topshop AS (
                                                        SELECT
                                                                am.product_name as amazon_product_name,
                                                                am.mrp,
                                                                tp.product_name,
                                                                tp.mrp
                                                        FROM
                                                                innerwear_amazon_com am
                                                                JOIN
                                                                innerwear_topshop_com tp
                                                        ON
                                                                am.product_name = tp.product_name 
                                                                AND
                                                                am.mrp = tp.mrp 
                    )
                    SELECT 
                            product_name ,
                            mrp 
                    FROM
                            innerwear_amazon_com iac
                    WHERE 
                            product_name NOT IN (select amazon_product_name from amazon_exclusive_macy) AND
                            product_name NOT IN (select amazon_product_name from amazon_exclusive_topshop);


	
	
	
                            CHATGPT Solution 1
	
                                SELECT
                                        am.product_name,
                                        am.mrp,
                                        macy.product_name,
                                        macy.mrp,
                                        tp.product_name ,
                                        tp.mrp 
                                FROM
                                        innerwear_amazon_com am
                                        LEFT JOIN
                                        innerwear_macys_com macy
                                ON
                                        am.product_name = macy.product_name 
                                        AND
                                        am.mrp = macy.mrp
                                        LEFT JOIN 
                                        innerwear_topshop_com tp
                                ON
                                        am.product_name = tp.product_name 
                                        AND
                                        am.mrp = tp.mrp 
                                WHERE
                                        macy.product_name IS NULL
                                        AND
                                        tp.product_name  IS NULL;
	
	
	
                            CHATGPT Solution 2

                                SELECT 
                                        am.product_name,
                                        am.brand_name,
                                        am.price,
                                        am.rating
                                FROM
                                        innerwear_amazon_com am
                                WHERE NOT EXISTS (
                                                    SELECT 
                                                            1 
                                                    FROM
                                                            innerwear_macys_com mc
                                                    WHERE
                                                            mc.product_name = am.product_name 
                                                            AND
                                                            mc.mrp = am.mrp
                                )
                                AND NOT EXISTS (
                                                        SELECT 
                                                                1
                                                        FROM
                                                            innerwear_topshop_com tp
                                                        WHERE
                                                            tp.product_name = am.product_name 
                                                            AND
                                                            tp.mrp = am.mrp 
                                )


Pyspark solution will be according to chatgpt solution 1
my solution is not very optimal or accurate

"""




from pyspark.sql import SparkSession
from pyspark.sql.functions import col,lit,when
import logging


logging.basicConfig(level = logging.INFO,format = "%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


if __name__ == "__main__":
    logger.info("Starting Spark Application")

    
    try:
        spark = SparkSession.builder.appName("Spark-Problem24Amazon")\
                                .config("spark.ui.port","4041")\
                                .config("spark.jars","file:///C://Users/shree/Downloads/postgresql-42.7.4.jar")\
                                .master("local[*]")\
                                .getOrCreate()
        
        spark.sparkContext.setLogLevel("WARN")


    except Exception as e:
        logger.error("Failed to create SparkSession",exec_info=True)
        raise
    
    #org.postgresql.Driver
    #jdbc:postgresql://{host}[:{port}]/[{database}]

    jdbc_url = "jdbc:postgresql://localhost:5432/postgres"
    query = "(select * from innerwear_amazon_com) AS query"
    connection_properties = {
                
                "user":"postgres",
                "password":"Pkts1t4j11@",
                "driver":"org.postgresql.Driver"
    }


    innerwear_amazon_com_df = spark.read.jdbc(url = jdbc_url,table=query,properties=connection_properties)

    query = "(select * from innerwear_macys_com) AS query"

    innerwear_macys_com_df = spark.read.jdbc(url = jdbc_url,table=query,properties=connection_properties)

    query = "(select * from innerwear_topshop_com) AS query"

    innerwear_topshop_com_df = spark.read.jdbc(url = jdbc_url,table=query,properties=connection_properties)


    joineddf = innerwear_amazon_com_df.alias("am").join(innerwear_macys_com_df.alias("mc"),(col("am.product_name") == col("mc.product_name")) & (col("am.mrp") == col("mc.mrp")),how="left")\
                                                  .join(innerwear_topshop_com_df.alias("tp"),(col("am.product_name") == col("tp.product_name")) & (col("am.mrp") == col("tp.product_name")),how = "left")\
                                                  .where(col("mc.product_name").isNull() & col("tp.product_name").isNull())\
                                                  .select(col("am.product_name"),col("am.brand_name"),col("am.price"),col("am.rating"))
    
    joineddf.show(truncate=False)

    


    logger.info("Ending Spark Application")