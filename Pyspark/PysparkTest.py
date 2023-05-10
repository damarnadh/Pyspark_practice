from pyspark.sql import SparkSession


sparkSession = SparkSession.builder.appName("test").getOrCreate()
columns =["seqno","names"]
data =[(1,"damar"),(2,"chinnu"),(3,"ammu")]
sparkSession.createDataFrame(data=data,schema= columns).show(truncate=False)

print("success")

#creating new session
spark2 = sparkSession.newSession
print(spark2)

#adding existing session
spark3 = sparkSession.builder.getOrCreate()
print(spark3)

#config method
sparkSession = SparkSession.\
    builder.\
    master("local[3]").\
    appName("test").\
    config("config-test","config-test-value").\
    getOrCreate()

spark_hive = SparkSession. \
    builder. \
    master("local[3]"). \
    appName("test"). \
    config("config-test","config-test-value"). \
    enableHiveSupport().\
    getOrCreate()

sparkSession.conf.set("spark.executer.memory","5g")
partitions = sparkSession.conf.get("spark.sql.shuffle.partitions")
print(partitions)


# Create DataFrame
df = sparkSession.createDataFrame(
    [("Scala", 25000), ("Spark", 35000), ("PHP", 21000)])
df.show()

# Output
#+-----+-----+
#|   _1|   _2|
#+-----+-----+
#|Scala|25000|
#|Spark|35000|
#|  PHP|21000|
#+-----+-----+


# Spark SQL
df.createOrReplaceTempView("sample_table")
df2 = sparkSession.sql("SELECT _1,_2 FROM sample_table")
df2.show()


# Create Hive table & query it.
#sparkSession.table("sample_table").write.saveAsTable("sample_hive_table") #not working
#df3 = spark.sql("SELECT _1,_2 FROM sample_hive_table")
#df3.show()


# Get metadata from the Catalog
# List databases
dbs = sparkSession.catalog.listDatabases()
print(dbs)

# Output
#[Database(name='default', description='default database',
#locationUri='file:/Users/admin/.spyder-py3/spark-warehouse')]

# List Tables
tbls = sparkSession.catalog.listTables()
print(tbls)

#Output
#[Table(name='sample_hive_table', database='default', description=None, tableType='MANAGED', #isTemporary=False), Table(name='sample_hive_table1', database='default', description=None, #tableType='MANAGED', isTemporary=False), Table(name='sample_hive_table121', database='default', #description=None, tableType='MANAGED', isTemporary=False), Table(name='sample_table', database=None, #description=None, tableType='TEMPORARY', isTemporary=True)]
