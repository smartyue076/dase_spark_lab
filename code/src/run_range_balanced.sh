#!/bin/bash

APP_NAME="KMeans-Range-Balanced"
DATA_PATH="/workspace/spark-partition-exp/dataset/kmeans-balanced.txt"

$SPARK_HOME/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --name $APP_NAME \
  \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=file:/tmp/spark-events \
  --conf spark.history.fs.logDirectory=file:/tmp/spark-events \
  \
  --conf spark.executor.memory=1g \
  --conf spark.executor.cores=2 \
  \
  /workspace/spark-partition-exp/jobs/kmeans_partition_exp.py \
  --input $DATA_PATH \
  --strategy range \
  --num_partitions 4 \
  --tag $APP_NAME
