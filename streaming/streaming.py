from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql.functions import from_json
from pyspark.sql.functions import col, concat_ws
import pyspark.sql.types as tp
from pyspark.sql.functions import expr
from pyspark.sql.functions import to_timestamp
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.sql.types import IntegerType, StringType
from pyspark.ml.regression import LinearRegressionModel,LinearRegression
from pyspark.ml.feature import StringIndexerModel
from pyspark.ml import PipelineModel


kafkaServer="broker:29092"
topic = "data"

sparkConf = SparkConf().set("es.nodes", "elasticsearch") \
                        .set("es.port", "9200")\
                        #.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                        
elastic_index="tap"

spark = SparkSession.builder.appName("app").config(conf=sparkConf).getOrCreate()
"regione"
schema = tp.StructType([
    tp.StructField("numTreno", tp.StringType()),
    tp.StructField("regione", tp.StringType()),
    tp.StructField("@lt", tp.StringType()),
    tp.StructField("ritardoArr", tp.StringType()),
    tp.StructField("oraPart", tp.StringType()),
    tp.StructField("categoria", tp.StringType()),
    tp.StructField("stazArr", tp.StringType()),
    tp.StructField("stazPart", tp.StringType()),
    tp.StructField("ritardoPart", tp.StringType()),
    tp.StructField("oraArr", tp.StringType()),
    tp.StructField("@timestamp", tp.StringType(), True)
])

# Leggere i dati da Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafkaServer) \
    .option("subscribe", topic) \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()



# Decodifica i messaggi Kafka utilizzando lo schema definito
df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", schema).alias("data")) \
    .select("data.numTreno", "data.@lt", "data.ritardoArr", "data.oraPart", "data.categoria", "data.stazArr","data.stazPart","data.ritardoPart","data.oraArr","data.@timestamp","data.regione") \


# 
df = df.select("categoria", "numTreno", "stazPart", "ritardoPart", "stazArr", "ritardoArr","oraPart","oraArr","regione").dropna()

df = df.withColumn("Id_treno", col("numTreno"))
df = df.withColumn("numTreno", col("numTreno").cast(IntegerType()))

df = df.withColumn("ritardoPart", col("ritardoPart").cast(IntegerType()))

df = df.withColumn("numTreno", col("numTreno").cast("double"))
df = df.withColumn("ritardoPart", col("ritardoPart").cast("double"))
df = df.withColumn("ritardoArr", col("ritardoArr").cast("double"))


lr_model = LinearRegressionModel.load("./rl")
#PipelineModel.load("rl")
indexer1 = StringIndexerModel.load("indexer1")
indexer2 = StringIndexerModel.load("indexer2")
indexer3 = StringIndexerModel.load("indexer3")

indexer1.setHandleInvalid("keep")
indexer2.setHandleInvalid("keep")
indexer3.setHandleInvalid("keep")
# Applicazione dei trasformatori sul DataFrame di streaming
df = indexer1.transform(df)
df = indexer2.transform(df)
df = indexer3.transform(df)


df.printSchema()

# Assemblaggio delle feature in un unico vettore
assembler = VectorAssembler(
    inputCols=["numTreno","ritardoPart", "textColumn1Index", "textColumn2Index", "categoriaIndex"],
    outputCol="features",
    handleInvalid="skip"
)
df = assembler.transform(df)

#df_lr = df.select("features", "ritardoArr")


df.printSchema()
predictions = lr_model.transform(df)
predictions.printSchema()

#


df_d = predictions.select("categoria","regione", "numTreno", "stazPart", "ritardoPart", "stazArr", "ritardoArr","prediction","oraPart","oraArr","Id_treno")



#df = df.withColumn("id", expr("numTreno"))


       #.withColumn("timestampPart", to_timestamp("oraPart", "dd/MM/yyyy HH:mm:ss")) \
       #.withColumn("timestampArr", to_timestamp("oraArr", "dd/MM/yyyy HH:mm:ss"))
#df= df.withColumn("timestamp",to_timestamp("oraPart", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))

#df = df.withColumn("oraPart", to_timestamp("oraPart", "dd/MM/yyyy HH:mm:ss")) \
#       .withColumn("oraArr", to_timestamp("oraArr", "dd/MM/yyyy HH:mm:ss"))


df_d.writeStream \
   .option("checkpointLocation", "/tmp/ck") \
   .format("es") \
   .start(elastic_index) \
   .awaitTermination()
#df_d.writeStream \
#    .format("console") \
#    .start() \
#    .awaitTermination()