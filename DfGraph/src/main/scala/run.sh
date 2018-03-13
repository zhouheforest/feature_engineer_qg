#!/bin/bash


nohup /bin/spark2-submit --class com.quantgroup.ml.graph.DfOfUsers  --master yarn \
--deploy-mode cluster --driver-memory 40g --executor-memory 40G --num-executors 100 \
--executor-cores 3  --conf spark.default.parallelism=10000 \
--conf spark.storage.memoryFraction=0.3 --conf spark.driver.maxResultSize=10G \
--conf spark.yarn.maxAppAttempts=1 --conf spark.yarn.driver.memoryOverhead=20000 \
--conf spark.network.timeout=420000s --conf spark.executor.heartbeatInterval=200s \
--conf spark.io.compression.codec=snappy --conf spark.shuffle.compress=true \
--conf spark.dynamicAllocation.executorIdleTimeout=3600s \
--conf spark.shuffle.blockTransferService=nio --conf spark.shuffle.memoryFraction=0.6 \
--conf spark.storage.memoryFraction=0.2 --conf spark.yarn.driver.memoryOverhead=40000 \
Graph-1.0-SNAPSHOT.jar >count.out &
