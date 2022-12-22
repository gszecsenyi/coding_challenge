list = df.rdd.flatMap(lambda x: x).filter(lambda x: x!= None).distinct().map(lambda x: 'product ' + x).collect()
