#!/bin/bash

hdfs dfs -mkdir /user
hdfs dfs -mkdir /user/root
hdfs dfs -put /root/data /user/root

hdfs dfs -put /root/example_src/spark-demo/config.json config.json