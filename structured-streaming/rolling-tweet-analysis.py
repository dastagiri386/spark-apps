import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

if __name__ == "__main__":

	spark = SparkSession.builder.appName("Rolling-tweet-analytics").getOrCreate()
	spark.conf.set("spark.executor.memory", "1g")
	spark.conf.set("spark.executor.cores","4")
	spark.conf.set("spark.task.cpus","1")
	spark.conf.set("spark.eventLog.enabled","true")
	spark.conf.set("spark.eventLog.dir","hdfs://10.254.0.19:8020/event-log/")
	
	input_dir = sys.argv[1]

	activitySchema = StructType([StructField('userA', StringType(), True), StructField('userB', StringType(), True), StructField('timestamp', TimestampType(), True), StructField('interaction', StringType(), True)])
	
	df = spark.readStream.option("sep", ",").schema(activitySchema).csv(input_dir)
	windowedCounts = df.groupBy(window(df.timestamp, "60 minutes", "30 minutes"), df.interaction).count().orderBy("window")
	
	query = windowedCounts.writeStream.queryName("partA1").outputMode("complete").format("console").option("truncate", "false").option("numRows", 1000000000).start()
	query.awaitTermination()




