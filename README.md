# spark-apps

## [PageRank](https://en.wikipedia.org/wiki/PageRank)

Dataset : [Berkeley-Stanford web graph](https://snap.stanford.edu/data/web-BerkStan.html)
Different implementations of PageRank
* PageRank without any custom partitioning or RDD persistence. For full cluster utilization, choose number of partitions optimally
* PageRank with custom partitioning (hash partitioned on URL)
* PageRank with appropriate RDDs persisted in memory (linksRDD)

```shell
$ ~/software/spark-2.1.0-bin-hadoop2.6/bin/spark-submit --master spark://10.254.0.19:7077  --driver-memory 1g <NAME_OF_THE_FILE>.py "hdfs://10.254.0.19:8020/user/ubuntu/web-BerkStan.txt" "10"
```

## [Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)

Dataset : [Higgs Twitter Dataset](https://snap.stanford.edu/data/higgs-twitter.html)
* Pyspark [application](https://github.com/dastagiri386/spark-apps/blob/master/structured-streaming/rolling-tweet-analysis.py) that emits the number of retweets (RT), mention (MT), reply (RE) for an hourly window that is updated every 30 minutes based on timestamps of tweets
* [Application](https://github.com/dastagiri386/spark-apps/blob/master/structured-streaming/tweet-mentions.py) to emit twitter IDs of user mentioned by other users every 10 seconds
* [Application](https://github.com/dastagiri386/spark-apps/blob/master/structured-streaming/batch-stream-tweet-mashup.py) that mixes statis data and streaming computations. The application takes as input the list of twitter user IDs and every 5 seconds , it emits the number of tweet actions of user if present in list.


