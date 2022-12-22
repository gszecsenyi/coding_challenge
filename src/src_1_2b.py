c = df.rdd.flatMap(lambda x: x).filter(lambda x: x!= None).count()
