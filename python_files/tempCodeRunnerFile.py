columns}")
	orders_df.filter(col('order_id').isNotNull()).show()
	orders_df.filter(col('customer_id').isNotN