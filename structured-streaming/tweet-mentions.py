import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

if __name__ == "__main__":

	spark = SparkSession.builder.appName("Mentions").getOrCreate()
	spark.conf.set("spark.executor.memory", "1g")
	spark.conf.set("spark.executor.cores","4")
	spark.conf.set("spark.task.cpus","1")
	spark.conf.set("spark.eventLog.enabled","true")
	spark.conf.set("spark.eventLog.dir","hdfs://10.254.0.19:8020/event-log/")
	spark.conf.set("spark.sql.streaming.checkpointLocation",sys.argv[3])	
	input_dir = sys.argv[1]
	output_dir = sys.argv[2]

	activitySchema = StructType([StructField('userA', StringType(), True), StructField('userB', StringType(), True), StructField('timestamp', TimestampType(), True), StructField('interaction', StringType(), True)])
	
	df = spark.readStream.option("sep", ",").schema(activitySchema).csv(input_dir)
	mentionedUsers = df.select("userB").where("interaction = 'MT'")

	query = mentionedUsers.writeStream.format("parquet").trigger(processingTime='10 seconds').start(output_dir)
	query.awaitTermination()




