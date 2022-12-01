#!/usr/bin/env bash
mvn clean install
hadoop jar target/demo-1.0.jar iitp.App data output columns.json 
hdfs dfs -get output
hdfs dfs -rm -r -skipTrash output
