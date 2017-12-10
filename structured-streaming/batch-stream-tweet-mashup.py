import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

if __name__ == "__main__":

	spark = SparkSession.builder.appName("Batch-stream-tweet-mashup").getOrCreate()
	spark.conf.set("spark.executor.memory", "1g")
	spark.conf.set("spark.executor.cores","4")
	spark.conf.set("spark.task.cpus","1")
	spark.conf.set("spark.eventLog.enabled","true")
	spark.conf.set("spark.eventLog.dir","hdfs://10.254.0.19:8020/event-log/")
	
	input_dir = sys.argv[1]
	input_list = sys.argv[2]

	activitySchema = StructType([StructField('userA', StringType(), True), StructField('userB', StringType(), True), StructField('timestamp', TimestampType(), True), StructField('interaction', StringType(), True)])
	
	df = spark.readStream.option("sep", ",").schema(activitySchema).csv(input_dir)
	inputList = spark.sparkContext.textFile(input_list).map(lambda x: x.split(',')).first()
	
	userActions = df.where(col("userA").isin(inputList)).select("userA").groupBy("userA").count()
	query = userActions.writeStream.outputMode("complete").format("console").trigger(processingTime='5 seconds').option("truncate", "false").option("numRows", "2000000000").start()
	query.awaitTermination()
	



