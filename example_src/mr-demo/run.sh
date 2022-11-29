#!/usr/bin/env bash
mvn clean install
hadoop jar target/demo-1.0.jar iitp.App data data/output columns.json 
hdfs dfs -get data/output
hdfs dfs -rm -r -skipTrash data/output
