from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql.functions import from_json
from pyspark.sql.functions import col, concat_ws
import pyspark.sql.types as tp
from pyspark.sql.functions import expr
from pyspark.sql.functions import to_timestamp


kafkaServer="broker:29092"
topic = "data"

sparkConf = SparkConf().set("es.nodes", "elasticsearch") \
                        .set("es.port", "9200")
elastic_index="tap"

spark = SparkSession.builder.appName("app").config(conf=sparkConf).getOrCreate()

schema = tp.StructType([
    tp.StructField("numTreno", tp.StringType()),
    tp.StructField("variazioni", tp.StringType()),
    tp.StructField("@lt", tp.StringType()),
    tp.StructField("ritardoArr", tp.StringType()),
    tp.StructField("oraPart", tp.StringType()),
    tp.StructField("categoria", tp.StringType()),
    tp.StructField("stazArr", tp.StringType()),
    tp.StructField("stazPart", tp.StringType()),
    tp.StructField("ritardoPart", tp.StringType()),
    tp.StructField("oraArr", tp.StringType()),
    tp.StructField("provvedimenti", tp.StringType()),
    tp.StructField("@timestamp", tp.StringType(), True)
])

# Leggere i dati da Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafkaServer) \
    .option("subscribe", topic) \
    .option("startingOffsets", "earliest") \
    .load()

# Decodifica i messaggi Kafka utilizzando lo schema definito
df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", schema).alias("data")) \
    .select("data.numTreno", "data.variazioni", "data.@lt", "data.ritardoArr", "data.oraPart", "data.categoria", "data.stazArr","data.stazPart","data.ritardoPart","data.oraArr","data.provvedimenti","data.@timestamp") \


df = df.withColumn("id", expr("numTreno"))
       #.withColumn("timestampPart", to_timestamp("oraPart", "dd/MM/yyyy HH:mm:ss")) \
       #.withColumn("timestampArr", to_timestamp("oraArr", "dd/MM/yyyy HH:mm:ss"))
#df= df.withColumn("timestamp",to_timestamp("oraPart", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))

#df = df.withColumn("oraPart", to_timestamp("oraPart", "dd/MM/yyyy HH:mm:ss")) \
#       .withColumn("oraArr", to_timestamp("oraArr", "dd/MM/yyyy HH:mm:ss"))

#df.writeStream \

#     .format("console") \
#     .start() \
#     .awaitTermination()

df.writeStream \
   .option("checkpointLocation", "/tmp/") \
   .option("es.mapping.id", "id") \
   .format("es") \
   .start(elastic_index) \
   .awaitTermination()