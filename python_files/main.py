from schema import *
import pyspark.sql.functions as F
from pyspark.sql.session import SparkSession
import logging
from datetime import datetime
import os
import sys
from utils import *
from schema import *
import builtins

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
	# customer_df.filter(F.col('customer_id').isNull()).show()
	# customer_df.filter(F.col('customer_id').isNotNull()).groupBy(F.col('customer_id')).agg(F.count("*").alias("count_customer_id")).orderBy(F.col("customer_id").desc()).show()
	# customer_df.filter(F.col('customer_unique_id').isNull()).show()

	# logging.info(f"Orders columns: {orders_df.columns}")
	# orders_df.filter(F.col('order_id').isNull()).show()
	# CHECK DUPLICATES
	# orders_df.filter(F.col('order_id').isNotNull()).groupBy(F.col('order_id')).agg(F.count("*").alias("c_order_id")).orderBy(F.col("order_id").desc()).show()
	# orders_df.filter(F.col('customer_id').isNull()).show()

	# logging.info(f"Items columns: {items_df.columns}")
	# items_df.filter(F.col('order_id').isNotNull()).show()
	# items_df.filter(F.col('order_item_id').isNotNull()).select(F.col('order_item_id')).distinct().show()

	logging.info(f"Payments columns: {payment_df.columns}")
	# payment_df.filter(F.col('payment_value').isNull()).show()
	# payment_df.filter(F.col('order_item_id').isNotNull()).select(F.col('order_item_id')).distinct().show()
	# payment_df.groupBy(F.col("order_id")).agg(F.count("*").alias("no_of_orders")).orderBy(F.col("no_of_orders").desc()).show()
	# sys.exit(0)

	# ================================
	# Null Value Handling
	# ================================
	# customer_df = customer_df.fillna({ 	"customer_unique_id":"NA", 	"customer_zip_code_prefix":"00000", 	"customer_city":"NA", 	"customer_state":"NA"})
	# customer_df.filter(F.col("customer_city")=="NA").show()

	# Filling Null Amount values with 0
	items_df = items_df.fillna({"freight_value":0})
	logging.info("Filled items_df - null freight_value with 0")

	payment_df = payment_df.fillna({"payment_value":0})
	logging.info("Filled payment_df - null payment_value with 0")

	# ================================
	# Business Requirements
	# ================================

	# Creating Base data
	custkpi_cols = ["customer_id","customer_unique_id","customer_city","customer_state"]
	cust_kpi = customer_df.select(*custkpi_cols)

	orderskpi_cols = ["order_id","customer_id","order_purchase_timestamp"]
	orders_kpi = orders_df.select(*orderskpi_cols)

	paymentskpi_cols = ["order_id","payment_value"]
	payments_kpi = payment_df.select(*paymentskpi_cols)

	
	itemskpi_cols = ["order_id","product_id"]
	items_kpi = items_df.select(*itemskpi_cols)

	productskpi_cols = ["product_id","product_category_name"]
	products_kpi = products_df.select(*productskpi_cols)

	logging.info(f"Joining all the required dataframes for base data.")
	logging.info(f"Count of records before any join - {cust_kpi.count()}.")


	cust_orders_items_products_payments_df = cust_kpi.alias("cust").join(orders_kpi.alias("orders"),
											( F.col("cust.customer_id") == F.col("orders.customer_id")  ),
											how = "inner" ).drop(F.col("orders.customer_id"))\
									.join(items_kpi.alias("items"),
											( F.col("orders.order_id") == F.col("items.order_id")  ),
											how = "inner" ).drop(F.col("items.order_id"))\
									.join(products_kpi.alias("products"),
			   								( F.col("items.product_id") == F.col("products.product_id")  ),
											how = "inner" ).drop(F.col("products.product_id"))\
									.join(payments_kpi.alias("payments"),
			   								( F.col("orders.order_id") == F.col("payments.order_id")  ),
											how = "inner" ).drop(F.col("payments.order_id"))
	# cust = 99441
	# cust + orders = 99441
	# cust + orders + items = 112650 (Single order can have multiple items)
	# cust + orders + items + products = 112650
	# cust + orders + items + products + payments = 117601 (Single order can be paid in multiple payments)

	logging.info(f"Count of records after all the joins - {cust_orders_items_products_payments_df.count()}.")


	# --------------------------------
	# 1. Identify top customers by revenue
	# --------------------------------
	logging.info("Processing - 1. Identify top customers by revenue")
									
	cust_orders_payments_df_kpi1_sel = cust_orders_items_products_payments_df.select(*["customer_unique_id", "payment_value"])
	kpi1_df = cust_orders_payments_df_kpi1_sel.groupBy(F.col("customer_unique_id")).agg(F.round(F.sum(F.col("payment_value")),2).alias("revenue"))
	kpi1_df = kpi1_df.orderBy(F.col("revenue").desc())

	# kpi1_df.show(5, truncate=False)
	logging.info("Writing the Top Customer's revenue data into 1_top_customers_revenue folder")
	# kpi1_df.coalesce(1).write.mode("overwrite").option("header", True).csv("output/1_top_customers_revenue")


	# --------------------------------
	# 2. Generate monthly revenue trends
	# --------------------------------
	logging.info("Processing - 2. Generate monthly revenue trends")
	# orders_kpi2 = orders_df.select(*["order_id","order_purchase_timestamp"])
	# payment_kpi2 = payment_df.select(*["order_id","payment_value"])
	
	orders_payment_df = cust_orders_items_products_payments_df.select(*["order_purchase_timestamp","payment_value"])
	orders_payment_df = orders_payment_df.withColumn("order_purchase_year_month", F.date_format(F.col("order_purchase_timestamp"),'yyyy-MMM') )

	kpi2_df = orders_payment_df.groupBy(F.col("order_purchase_year_month")).agg(F.round(F.sum(F.col("payment_value")),2).alias("revenue"))
	kpi2_df = kpi2_df.orderBy(F.col("order_purchase_year_month").asc())

	# kpi2_df.show(truncate=False)
	logging.info("Writing the Top Customer's revenue data into 2_monthly_revenue folder")
	# kpi2_df.coalesce(1).write.mode("overwrite").option("header", True).csv("output/2_monthly_revenue")

	# --------------------------------
	# 3. Analyze revenue by city and state
	# --------------------------------
	logging.info("Processing - 3. Analyze revenue by city and state")
	cust_orders_payments_df_kpi3_sel = cust_orders_items_products_payments_df.select(*["customer_city","customer_state","payment_value"])

	kpi3_df_a = cust_orders_payments_df_kpi3_sel.groupBy(F.col("customer_city"), F.col("customer_state")).agg(F.round(F.sum(F.col("payment_value")),2).alias("revenue"))

	kpi3_df_b = cust_orders_payments_df_kpi3_sel.groupBy(F.col("customer_state")).agg(F.round(F.sum(F.col("payment_value")),2).alias("revenue"))

	# kpi3_df_a.show(5, truncate = False)
	# kpi3_df_b.show(5, truncate = False)

	logging.info("Writing the Top Customer's revenue data into 3a_state_city_wise_revenue folder")
	# kpi3_df_a.coalesce(1).write.mode("overwrite").option("header", True).csv("output/3a_state_city_wise_revenue")

	logging.info("Writing the Top Customer's revenue data into 3b_state_wise_revenue folder")
	# kpi3_df_b.coalesce(1).write.mode("overwrite").option("header", True).csv("output/3b_state_wise_revenue")

	# --------------------------------
	# 4. Find most used payment methods
	# --------------------------------
	logging.info("Processing - 4. Find most used payment methods")

	most_used_payment_method_df = payment_df.groupBy(F.col("payment_type")).agg(F.count("*").alias("cnt_payment_method"), F.sum(F.col("payment_value")).alias("payment_value"))\
		.orderBy(F.col("cnt_payment_method").desc()).limit(1)
	# most_used_payment_method_df.show()

	most_used_payment_method_row = most_used_payment_method_df.first()

	most_used_payment_method = most_used_payment_method_row["payment_type"]
	most_used_payment_method_count = most_used_payment_method_row["cnt_payment_method"]
	most_used_payment_method_value = builtins.round(float(most_used_payment_method_row["payment_value"]),2)

	logging.info(f"Find most used payment methods : \"{most_used_payment_method}\" used for {most_used_payment_method_count} times contributing value of {most_used_payment_method_value}")


	# --------------------------------
	# 5. Identify delayed orders
	# --------------------------------
	logging.info("Processing - 5. Identify delayed orders")

	required_columns_from_orders_for_delay = ["order_id","customer_id","order_status","order_delivered_customer_date","order_estimated_delivery_date", "dalay_in_days"]
	delayed_orders_df = orders_df.filter( (F.col("order_status") == "delivered" ) & (F.col("order_delivered_customer_date") > F.col("order_estimated_delivery_date")) )\
								.withColumn("dalay_in_days", F.datediff(F.col("order_delivered_customer_date"), F.col("order_estimated_delivery_date")))\
								.select(*required_columns_from_orders_for_delay)

	# delayed_orders_df.show()
	

	delayed_orders_info_df = delayed_orders_df.groupBy().agg(F.count("*").alias("cnt_delayed_orders"),
														  F.avg("dalay_in_days").alias("avg_days_delay"),
														  F.max("dalay_in_days").alias("max_days_delay"),
														  F.min("dalay_in_days").alias("min_days_delay"))
	
	delayed_orders_info_row = delayed_orders_info_df.first()

	delayed_orders_count = delayed_orders_info_row['cnt_delayed_orders']
	delayed_orders_avg = delayed_orders_info_row['avg_days_delay']
	delayed_orders_min = delayed_orders_info_row['max_days_delay']
	delayed_orders_max = delayed_orders_info_row['min_days_delay']


	logging.info(f"Total delayed order count is {delayed_orders_count}, with average delay = {delayed_orders_avg}, min delay = {delayed_orders_min} and max delay = {delayed_orders_max}")

	logging.info(f"Writing the delayed orders data into 5_delayed_orders_data folder.")

	# delayed_orders_df.coalesce(1).write.mode("overwrite").option("header", True).csv("output/5_delayed_orders_data/")


	# --------------------------------
	# 6. Generate top-selling product categories
	# --------------------------------
	logging.info("Processing - 6. Generate top-selling product categories")
	top_selling_prod_cat_df = cust_orders_items_products_payments_df.select(*["product_category_name", "payment_value"])
	top_selling_prod_cat_df = top_selling_prod_cat_df.groupBy(F.col("product_category_name"))\
													.agg(F.round(F.sum(F.col("payment_value")),2).alias("revenue"))\
													.orderBy(F.col("revenue").desc())

	logging.info(f"Writing the delayed orders data into 6_top_selling_prod_category_data folder.")

	# top_selling_prod_cat_df.coalesce(1).write.mode("overwrite").option("header", True).csv("output/6_top_selling_prod_category_data/")

	# --------------------------------
	# 7. Track customer order frequency
	# --------------------------------
	logging.info("Processing - 7. Track customer order frequency")
	customer_order_freq_df = cust_orders_items_products_payments_df.select(*["customer_unique_id", "order_id"])
	customer_order_freq_df = customer_order_freq_df.groupBy(F.col("customer_unique_id"))\
													.agg(F.count(F.col("order_id")).alias("order_frequency"))\
													.orderBy(F.col("order_frequency").desc())

	logging.info(f"Writing the delayed orders data into 7_top_customer_order_frequency folder.")

	# customer_order_freq_df.coalesce(1).write.mode("overwrite").option("header", True).csv("output/7_top_customer_order_frequency/")



	




	