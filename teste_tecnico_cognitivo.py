from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import col
from pyspark.sql.session import SparkSession
from pyspark.sql import SQLContext
import json


sc = SparkSession.builder.appName("Process Pipeline").getOrCreate()
spark = SparkSession(sc)
sqlContext = SQLContext(sc)

df1 = spark.read.format("csv").option("inferSchema", "false").option("header", "true").load("/home/ubuntu/data-master/data/input/users/load.csv")

df1.write.orc("/home/ubuntu/data-master/data/output/exercicio1")


df1.registerTempTable("deduplicar")

df2 = sqlContext.sql("SELECT id, name, email ,phone ,address ,age, create_date, update_date  FROM( SELECT id, name, email ,phone ,address ,age, create_date, update_date, ROW_NUMBER()OVER(PARTITION BY id ORDER BY update_date desc) rn FROM deduplicar ) y where rn = 1 order by id")

spark.catalog.dropTempView("deduplicar")

df2.write.orc("/home/ubuntu/data-master/data/output/exercicio2")


with open('/home/ubuntu/data-master/config/types_mapping.json') as json_config:  
       dataType = json.load(json_config)
       for dt in dataType:
           df3 = df2.withColumn(dt,col(dt).cast(dataType[dt]))


df3.write.orc("/home/ubuntu/data-master/data/output/exercicio3")



