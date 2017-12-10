import re
import sys
from operator import add

from pyspark.sql import SparkSession

def computeContribs(urls, rank):
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)

def parseNeighbors(urls):
    #if urls.startswith('#'):
    parts = re.split(r'\s+', urls)
    return parts[0], parts[1]

	
if __name__ == "__main__":
    # Initialize the spark context.

    spark = SparkSession.builder.appName("Pagerank-with-custom-partitioning").getOrCreate()

    spark.conf.set("spark.executor.memory", "1g")
    spark.conf.set("spark.executor.cores","4")
    spark.conf.set("spark.task.cpus","1")
    spark.conf.set("spark.eventLog.enabled","true")
    spark.conf.set("spark.eventLog.dir","hdfs://10.254.0.19:8020/event-log/")
    
    # create lines RDD, filter out lines that start with #
    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0]).filter(lambda line: not line.startswith('#') == True)

    # Loads all URLs from input file and initialize their neighbors.
    # We define custom hash partitioning here based on URL and define 16 partitions
    links = lines.map(lambda urls: parseNeighbors(urls)).distinct().groupByKey().partitionBy(16, lambda k: int(k) % 16)

    # Loads all URLs with other URL(s) link to from input file and initialize ranks of them to one.
    ranks = links.map(lambda url_neighbors: (url_neighbors[0], 1.0))

    # Calculates and updates URL ranks continuously using PageRank algorithm.
    for iteration in range(int(sys.argv[2])):
        # Calculates URL contributions to the rank of other URLs.
        contribs = links.join(ranks).flatMap(
            lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))

        # Re-calculates URL ranks based on neighbor contributions.
        # define hash partitioning for ranks RDD (same as lines RDD)
        ranks = contribs.reduceByKey(add).partitionBy(16, lambda k : int(k) %16).mapValues(lambda rank: rank * 0.85 + 0.15)

    # Collects all URL ranks and dump them to console.
    for (link, rank) in ranks.collect():
        print("%s has rank: %s." % (link, rank))

    spark.stop()

