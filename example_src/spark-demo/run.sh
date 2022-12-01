#!/usr/bin/env bash
mvn clean install
spark-submit --master yarn --class iitp.App target/demo-1.0.jar config.json
hdfs dfs -get output
hdfs dfs -rm -r -skipTrash output
