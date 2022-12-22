df_p2.filter("review_scores_value = 10").filter("price > 5000").select(f.avg("bathrooms").alias("avg_bathrooms"),f.avg("bedrooms").alias("avg_bedrooms")).show()
