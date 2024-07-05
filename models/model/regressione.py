from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, StringType
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import StringIndexerModel

spark = SparkSession.builder.appName("Linear Regression with Spark MLlib").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


schema = (
    "categoria STRING, numTreno INT, stazPart STRING, oraPart STRING, ritardoPart INT, "
    "stazArr STRING, oraArr STRING, ritardoArr INT, provvedimenti STRING, variazioni STRING"
)


df = spark.read.csv("./combined_trains_data.csv", header=True, schema=schema)


df = df.select("categoria", "numTreno", "stazPart", "ritardoPart", "stazArr", "ritardoArr").dropna()


#df = df.withColumn("textColumn1", col("stazPart"))
#df = df.withColumn("textColumn2", col("stazArr"))
#df = df.withColumn("textColumn3", col("categoria"))

indexer1 = StringIndexer(inputCol="stazPart", outputCol="textColumn1Index")
indexer2 = StringIndexer(inputCol="stazArr", outputCol="textColumn2Index")
indexer3 = StringIndexer(inputCol="categoria", outputCol="categoriaIndex")
df = df.withColumn("numTreno", col("numTreno").cast(IntegerType()))

i1 = indexer1.fit(df)
i2 = indexer2.fit(df)
i3 = indexer3.fit(df)

df = i1.transform(df)
df = i2.transform(df)
df = i3.transform(df)

i1.write().overwrite().save("model/indexer1")
i2.write().overwrite().save("model/indexer2")
i3.write().overwrite().save("model/indexer3")

assembler = VectorAssembler(
    inputCols=["numTreno", "ritardoPart", "textColumn1Index", "textColumn2Index", "categoriaIndex"],
    outputCol="features"
)
df = assembler.transform(df)


df_lr = df.select("features", "ritardoArr")


train_data, test_data = df_lr.randomSplit([0.8, 0.2], seed=1234)
print("allenamento ...")

lr = LinearRegression(featuresCol="features", labelCol="ritardoArr",regParam=0.1, elasticNetParam=0.5)
lr_model = lr.fit(train_data)


predictions = lr_model.transform(test_data)


evaluator = RegressionEvaluator(predictionCol="prediction", labelCol="ritardoArr", metricName="rmse")
rmse = evaluator.evaluate(predictions)
print(f"Root Mean Squared Error (RMSE) on test data = {rmse}")

import os
model_save_path = "model/rl"

lr_model.write().overwrite().save(model_save_path)

predictions.select("ritardoArr", "prediction", "features").show(10, truncate=False)


spark.stop()
