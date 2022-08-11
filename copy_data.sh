#!/usr/bin/env bash

# create directories to copy KPI data from Apache Hadoop Distributed File System (HDFS)

mkdir -p {~/calculate_kpis/time_based_kpis,~/calculate_kpis/time_country_based_kpis}

# copy data from HDFS to local directories

hdfs dfs -copyToLocal /user/livy/calculate_kpis/time_based_kpis/*.json ~/calculate_kpis/time_based_kpis
hdfs dfs -copyToLocal /user/livy/calculate_kpis/time_country_based_kpis/*.json ~/calculate_kpis/time_country_based_kpis

# pack the data into a Output.zip file

zip ~/Output.zip ~/console-output.txt -r ~/calculate_kpis