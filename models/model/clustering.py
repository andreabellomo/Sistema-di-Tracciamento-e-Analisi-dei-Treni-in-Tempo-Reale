from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType
from pyspark.ml.feature import VectorAssembler, StringIndexer, StandardScaler
from pyspark.ml.clustering import KMeans, KMeansModel

# Initialize Spark session
spark = SparkSession.builder.appName("MLP with Spark NLP").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Read the CSV file
df = spark.read.csv("./combined_trains_data.csv", header=True, inferSchema=True)

# Cast columns to integer type
df = df.withColumn("numTreno", col("numTreno").cast(IntegerType()))
df = df.withColumn("ritardoPart", col("ritardoPart").cast(IntegerType()))
df = df.withColumn("ritardoArr", col("ritardoArr").cast(IntegerType()))

# Add the new text columns (replace 'textColumn1' and 'textColumn2' with actual column names)
df = df.withColumn("textColumn1", col("stazPart"))
df = df.withColumn("textColumn2", col("stazArr"))

# Select relevant columns and drop rows with null values
df = df.select("numTreno", "ritardoArr", "ritardoPart", "textColumn1", "textColumn2").dropna()

# Print schema and a few rows to inspect the data
df.printSchema()
df.show(10)

# Filter out rows with null values (extra precaution)
df_ml = df.filter(df["numTreno"].isNotNull()).filter(df["ritardoArr"].isNotNull()).filter(df["ritardoPart"].isNotNull())

# Convert text columns to numerical indices using StringIndexer
indexer1 = StringIndexer(inputCol="textColumn1", outputCol="textColumn1Index")
indexer2 = StringIndexer(inputCol="textColumn2", outputCol="textColumn2Index")

df_ml = indexer1.fit(df_ml).transform(df_ml)
df_ml = indexer2.fit(df_ml).transform(df_ml)

# Print the transformed DataFrame schema and a few rows
df_ml.printSchema()
df_ml.show(10)

# Assemble features into a single vector
assembler = VectorAssembler(
    inputCols=["numTreno", "ritardoArr", "ritardoPart", "textColumn1Index", "textColumn2Index"],
    outputCol="features"
)
df_ml = assembler.transform(df_ml)

# Scale the features
scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures", withStd=True, withMean=False)
scalerModel = scaler.fit(df_ml)
df_ml = scalerModel.transform(df_ml).select("scaledFeatures")

# Print the scaled DataFrame schema and a few rows
df_ml.printSchema()
df_ml.show(10)

# Initialize and fit KMeans model with different values of k
print(f"Training KMeans with k={2}")
kmeans = KMeans(k=2, seed=1, maxIter=100, featuresCol="scaledFeatures")
model = kmeans.fit(df_ml)

model.setPredictionCol("newPrediction")
transformed = model.transform(df_ml).select("scaledFeatures", "newPrediction")

cluster_counts = transformed.groupBy("newPrediction").count()
cluster_counts.show()

print("Cluster Centers: ")
for center in model.clusterCenters():
    print(center)

# Save the model (optional)
model_path = "model/clus"
model.write().overwrite().save(model_path)

# Load the model and verify (optional)
model2 = KMeansModel.load(model_path)
print(model.clusterCenters()[0] == model2.clusterCenters()[0])
print(model.clusterCenters()[1] == model2.clusterCenters()[1])
print(model.transform(df_ml).take(1) == model2.transform(df_ml).take(1))

# Stop the Spark session
spark.stop()
