#!/usr/bin/env bash

# create directories to save KPI data and checkpoints in Apache Hadoop Distributed File System

hdfs dfs -mkdir -p /user/livy/calculate_kpis/time_based_kpis/checkpoints
hdfs dfs -mkdir -p /user/livy/calculate_kpis/time_country_based_kpis/checkpoints

# run the Spark Streaming application
# write or overwrite the standard output stream to console-output.txt

spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 calculate_kpis.py | tee ~/console-output.txt
