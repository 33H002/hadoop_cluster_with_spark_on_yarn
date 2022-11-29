#!/usr/bin/env bash
mvn clean install
spark-submit --class iitp.App target/demo-1.0.jar config.json
hdfs dfs -get data/output
hdfs dfs -rm -r -skipTrash data/output
