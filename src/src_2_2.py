from pyspark.sql import functions as f

df_p2.select(f.min("price").alias("min_price"),f.max("price").alias("max_price"), f.count("*").alias("row_count"))