from pyspark.sql import SparkSession


sparkSession = SparkSession.builder.appName("test").getOrCreate()
columns =["seqno","names"]
data =[(1,"damar"),(2,"chinnu"),(3,"ammu")]
sparkSession.createDataFrame(data=data,schema= columns).show(truncate=False)

print("success")
