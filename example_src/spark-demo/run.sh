#!/usr/bin/env bash
mvn clean install
spark-submit --master yarn --deploy-mode client --num-executors 3 --executor-cores 4 --executor-memory 16g --class iitp.App target/demo-1.0.jar config.json
#spark-submit --master yarn --deploy-mode client --num-executors 1 --executor-cores 12 --executor-memory 48g --class iitp.App target/demo-1.0.jar config.json
hdfs dfs -get output
hdfs dfs -rm -r -skipTrash output
