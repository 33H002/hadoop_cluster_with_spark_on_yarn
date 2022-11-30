#!/bin/bash

hdfs dfs -mkdir /user
hdfs dfs -mkdir /user/root
hdfs dfs -put /root/data /user/root

hdfs dfs -put /root/Workspace/example_src/demo/config.json config.json
