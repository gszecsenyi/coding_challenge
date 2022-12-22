from pyspark.sql import functions as f

df = spark.read.format("csv").load("file:/tmp/groceries.csv")
df_unpivoted = df.withColumn("idx", f.monotonically_increasing_id()).selectExpr("idx","stack(4,'_c0',_c0, '_c1', _c1, '_c2', _c2, '_c3', _c3)AS (column, product)").filter("product is not null").groupBy("idx").agg(f.collect_list("product").alias("elements")).select("elements")
temp_list = [i.elements for i in df_unpivoted.select("elements").collect()]


print(temp_list)
