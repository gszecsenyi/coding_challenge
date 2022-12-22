from pyspark.sql.types import StructType, StructField, DoubleType, StringType
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import LogisticRegression

schema = StructType([
    StructField("sepal_length", DoubleType(), True),
    StructField("sepal_width", DoubleType(), True),
    StructField("petal_length", DoubleType(), True),
    StructField("petal_width", DoubleType(), True),
    StructField("class", StringType(), True),

])

df_p3 = spark.read.csv("file:/tmp/iris.csv", schema)


feature_cols = df_p3.columns[:-1]
assembler = VectorAssembler(inputCols=feature_cols, outputCol='features')
df_p3 = assembler.transform(df_p3)

df_p3 = df_p3.select(['features', 'class'])
label_indexer = StringIndexer(inputCol='class', outputCol='label').fit(df_p3)
df_p3 = label_indexer.transform(df_p3)

df_p3 = df_p3.select(['features', 'label'])
print("Reading for machine learning")
#df_p3.show(10)

reg = 1e5
lr = LogisticRegression(regParam=reg)
model = lr.fit(df_p3)

#prediction = model.transform(df_p3)
#print("Prediction")
#prediction.show(10)

pred_data = spark.createDataFrame(
    [(5.1, 3.5, 1.4, 0.2),
     (6.2, 3.4, 5.4, 2.3)],
    ["sepal_length", "sepal_width", "petal_length", "petal_width"])

feature_cols2 = pred_data.columns[:-1]
assembler2 = VectorAssembler(inputCols=feature_cols, outputCol='features')
pred_data = assembler.transform(pred_data)

predection = model.transform(pred_data)
predection.show()