from pyspark.sql.types import *

# Common Data Types used
# StringType()
# IntegerType() -> Stores whole numbers (4 bytes - 2,147,483,648 to 2,147,483,647)
# LongType() -> Stores whole numbers with more range (8 bytes - 9,223,372,036,854,775,808 to 9,223,372,036,854,775,807)
# DoubleType() -> precision = 10, scale = 0
# DecimalType(18,2) -> DecimalType(precision, scale) 
# DateType()
# TimestampType()
# BooleanType()

# "customer_id","customer_unique_id","customer_zip_code_prefix","customer_city","customer_state"
customer_schema = StructType([
    StructField("customer_id", StringType(), False),
    StructField("customer_unique_id", StringType(), True),
    StructField("customer_zip_code_prefix", StringType(), True),
    StructField("customer_city", StringType(), True),
    StructField("customer_state", StringType(), True)
])

# "order_id","customer_id","order_status","order_purchase_timestamp","order_approved_at","order_delivered_carrier_date","order_delivered_customer_date","order_estimated_delivery_date"
orders_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("order_status", StringType(), True),
    StructField("order_purchase_timestamp", TimestampType(), True),
    StructField("order_approved_at", TimestampType(), True),
    StructField("order_delivered_carrier_date", DateType(), True),
    StructField("order_delivered_customer_date", DateType(), True),
    StructField("order_estimated_delivery_date", DateType(), True)
])

# "order_id","order_item_id","product_id","seller_id","shipping_limit_date","price","freight_value"
items_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("order_item_id", IntegerType(), False),
    StructField("product_id", StringType(), False),
    StructField("seller_id", StringType(), False),
    StructField("shipping_limit_date", TimestampType(), True),
    StructField("price", DateType(), True),
    StructField("freight_value", IntegerType(), True)
])

# "product_id","product_category_name","product_name_lenght","product_description_lenght","product_photos_qty","product_weight_g","product_length_cm","product_height_cm","product_width_cm"
products_schema = StructType([
    StructField("product_id", StringType(), False),
    StructField("product_category_name", StringType(), False),
    StructField("product_name_lenght", DecimalType(20, 5), False),
    StructField("product_description_lenght", DecimalType(20, 5), False),
    StructField("product_photos_qty", IntegerType(), False),
    StructField("product_weight_g", DecimalType(20, 5), False),
    StructField("product_length_cm", DecimalType(20, 5), False),
    StructField("product_height_cm", DecimalType(20, 5), False),
    StructField("product_width_cm", DecimalType(20, 5), False),
])

# "order_id","payment_sequential","payment_type","payment_installments","payment_value"
payment_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("payment_sequential", IntegerType(), False),
    StructField("payment_type", StringType(), False),
    StructField("payment_installments", IntegerType(), False),
    StructField("payment_value", DecimalType(20, 5), False)
])