#!/bin/bash
# ==========================================
# Stable KMeans Runner (3 datasets version)
# Matching actual dataset filenames
# ==========================================

SPARK_MASTER="spark://172.23.166.104:7077"

PY_FILE="/opt/spark/work-dir/lhy/code/src/kmeans_partition_exp.py"

# ****** IMPORTANT: match your actual dataset names ******
DATA_S0="/opt/spark/work-dir/lhy/code/dataset/kmeans-s0_uniform.txt"
DATA_S05="/opt/spark/work-dir/lhy/code/dataset/kmeans-s0.5_medium.txt"
DATA_S1="/opt/spark/work-dir/lhy/code/dataset/kmeans-s1_heavy.txt"

LOG_DIR="/tmp/spark-events"

NUM_WORKERS=4
EXECUTOR_CORES=1
EXECUTOR_MEMORY="1024m"

echo "========== Cluster Settings =========="
echo " Workers             = $NUM_WORKERS"
echo " Executor.cores      = $EXECUTOR_CORES"
echo " Executor.memory     = $EXECUTOR_MEMORY"
echo "======================================"
echo ""

run_job() {
    APP_NAME=$1
    STRATEGY=$2
    INPUT_PATH=$3

    echo "=============================="
    echo " Running $APP_NAME"
    echo " Strategy: $STRATEGY"
    echo " Input:    $INPUT_PATH"
    echo "=============================="

    $SPARK_HOME/bin/spark-submit \
        --master $SPARK_MASTER \
        --deploy-mode client \
        --name "$APP_NAME" \
        \
        --conf spark.eventLog.enabled=true \
        --conf spark.eventLog.dir=file:$LOG_DIR \
        \
        --conf spark.executor.instances=$NUM_WORKERS \
        --conf spark.executor.cores=$EXECUTOR_CORES \
        --conf spark.executor.memory=$EXECUTOR_MEMORY \
        \
        $PY_FILE \
        --input "$INPUT_PATH" \
        --strategy "$STRATEGY" \
        --num_partitions $NUM_WORKERS \
        --tag "$APP_NAME"

    echo ""
}

# ========== Run 6 experiments ==========
run_job "KMeans-Hash-S0"   "hash"  "$DATA_S0"
run_job "KMeans-Range-S0"  "range" "$DATA_S0"

run_job "KMeans-Hash-S05"  "hash"  "$DATA_S05"
run_job "KMeans-Range-S05" "range" "$DATA_S05"

run_job "KMeans-Hash-S1"   "hash"  "$DATA_S1"
run_job "KMeans-Range-S1"  "range" "$DATA_S1"

echo "======================================"
echo " All 6 KMeans jobs have completed!"
echo "======================================"
