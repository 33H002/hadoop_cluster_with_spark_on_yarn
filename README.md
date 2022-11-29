# iitp.2021-2022.demo

### MapReduce 및 Spark 기반 고속 가명처리, 결합 연산 데모 (공개용)

Docker multi-nodes Hadoop cluster with Spark 3.3.1 on Yarn. 

## Usage 
### Build 
```bash
make build
```
### Run 
```bash
make start
```
### Stop
```bash
make stop
```
### Connect to Master Node
```bash
make connect
```
 
### MapReduce
```bash 
cd /root/example_src/mr-demo
sh run.sh
```
or
```bash
cd /root/example_src/mr-demo
mvn clean install
hadoop jar target/demo-1.0.jar iitp.App data data/output columns.json 
```
### Spark submit 
```bash
cd /root/example_src/spark-demo
sh run.sh
```

or
```bash
cd /root/example_src/spark-demo
mvn clean install
spark-submit --class iitp.App target/demo-1.0.jar config.json
```

