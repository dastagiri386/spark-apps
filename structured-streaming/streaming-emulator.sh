#!/bin/bash

# the split dataset is already present in HDFS at hdfs:///user/ubuntu/workload/splitdataset
# For part A-1, We have created three folders created in HDFS under path '/user/ubuntu/workload'
# 1. splitDataset, contains all the split files
# 2. stageDir, data copied to stageDir before emulating the streams
# 3. monitorDir, data copied from stageDir to this directory thus emulating streaming data
data=/user/ubuntu/workload/splitDataset/
staging_dir=/user/ubuntu/workload/stageDir/
monitor_dir=/user/ubuntu/workload/monitorDir/
sleepTime=5s


hadoop fs -rm -r /user/ubuntu/workload/PartA-2-output/*
hadoop fs -rm -r /user/ubuntu/workload/PartA-2-checkpoint/*

echo Emptying staging directory...
hadoop fs -rm -r $staging_dir*

echo Copying data into staging directory...
hadoop fs -cp $data*.csv $staging_dir

echo Removing files from monitoring directory...
hadoop fs -rm -r $monitor_dir*


echo Moving data for streaming
for i in {1..1127}
do      
        path=$staging_dir$i.csv
        echo Streaming $path 
        hadoop fs -mv $path $monitor_dir/$i.csv 
        sleep $sleepTime
done
