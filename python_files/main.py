from schema import *
from pyspark.sql.functions import *
from pyspark.sql.session import SparkSession
import logging
from datetime import datetime
import os
from utils import *
from schema import *

def create_spark_session(spark_app_name, spark_master):
	logging.info("Creating spark session.")
	try:
		spark = SparkSession.builder.appName(spark_app_name).master(spark_master).getOrCreate()
		logging.info("Spark session has been created successfully.")
		return spark
	except Exception as e:
		logging.error("Error while creating the sparksession")
		sys.exit(1)


if __name__ == '__main__':
	# ================================
	# Reading config file
	# ================================
	config_file_path = os.path.join("config","config.ini")
	config_file_data = read_config_file(config_file_path, "PATH")

	config_file_path_data = dict(config_file_data["PATH"])
	source_path = config_file_path_data.get("source_path")
	log_path = config_file_path_data.get("log_path")
	output_path = config_file_path_data.get("output_path")

	config_file_spark_data = dict(config_file_data["SPARK"])
	spark_app_name = config_file_spark_data.get("app_name")
	spark_master = config_file_spark_data.get("master")

	# ================================
	# Calling logging method: level=INFO
	# ================================
	logging_config("main")
	logging.info("ETL started.")
	if config_file_data:
		logging.info("Config file read completed successfully.")

	# ================================
	# Creating the sparksession
	# ================================
	spark = create_spark_session(spark_app_name, spark_master)

	# ================================
	# Reading required source files
	# ================================

	# 1: CUSTOMER: olist_customers_dataset.csv
	customer_df = read_source_files(spark, os.path.join(source_path,"olist_customers_dataset.csv"), customer_schema)

	# 2: ORDER: olist_orders_dataset.csv
	orders_df = read_source_files(spark, os.path.join(source_path,"olist_orders_dataset.csv"), orders_schema)

	# 3: ITEM: olist_order_items_dataset.csv
	items_df = read_source_files(spark, os.path.join(source_path,"olist_order_items_dataset.csv"), items_schema)

	# 4: PRODUCTS: olist_products_dataset.csv
	products_df = read_source_files(spark, os.path.join(source_path,"olist_products_dataset.csv"), products_schema)

	# 5: PAYEMENT: olist_order_payments_dataset.csv
	payment_df = read_source_files(spark, os.path.join(source_path,"olist_order_payments_dataset.csv"), payment_schema)

	logging.info("Successfully loaded all the required files.")

	# ================================
	# Data Analysis ( Maintainance is optional in the flow and can be comment if not required )
	# ================================
	# customer_df, orders_df, items_df, products_df, payment_df

	# logging.info(str(i) for i in (customer_df.schema))
	# print(customer_df.printSchema())

	# logging.info(f"Customer columns: {customer_df.columns}")
	# CHECK NULLS
	# customer_df.filter(col('customer_id').isNull()).show()
	# customer_df.filter(col('customer_id').isNotNull()).groupBy(col('customer_id')).agg(count("*").alias("count_customer_id")).orderBy(col("customer_id").desc()).show()
	# customer_df.filter(col('customer_unique_id').isNull()).show()

	# logging.info(f"Orders columns: {orders_df.columns}")
	# orders_df.filter(col('order_id').isNull()).show()
	# CHECK DUPLICATES
	# orders_df.filter(col('order_id').isNotNull()).groupBy(col('order_id')).agg(count("*").alias("c_order_id")).orderBy(col("order_id").desc()).show()
	# orders_df.filter(col('customer_id').isNull()).show()

	# logging.info(f"Items columns: {items_df.columns}")
	# items_df.filter(col('order_id').isNotNull()).show()
	# items_df.filter(col('order_item_id').isNotNull()).select(col('order_item_id')).distinct().show()

	# ================================
	# Null Value Handling
	# ================================
	# customer_df = customer_df.fillna({ 	"customer_unique_id":"NA", 	"customer_zip_code_prefix":"00000", 	"customer_city":"NA", 	"customer_state":"NA"})
	# customer_df.filter(col("customer_city")=="NA").show()

	# Filling Null Amount values with 0
	items_df = items_df.fillna({"freight_value":0})
	logging.info("Filled items_df - null freight_value with 0")

	payment_df = payment_df.fillna({"payment_value":0})
	logging.info("Filled payment_df - null payment_value with 0")

	# ================================
	# Null Value Handling
	# ================================

	# 4. Find most used payment methods
	most_used_payment_method_df = payment_df.groupBy(col("payment_type")).agg(count("*").alias("cnt_payment_method")).orderBy(col("cnt_payment_method").desc()).limit(1)
	most_used_payment_method_df.show()

	most_used_payment_method = most_used_payment_method_df.first()["cnt_payment_method"]
	logging.info(f"Find most used payment methods : {most_used_payment_method}")



	