#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
用法:
/opt/spark/bin/spark-submit \
  --master spark://172.23.166.104:7077 \
  --executor-memory 1G \
  --executor-cores 1 \
  --num-executors 4 \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=file:///tmp/spark-events \
  --conf spark.sql.shuffle.partitions=4 \
  /opt/spark/work-dir/xy/dase_spark_lab/code/src/word_count.py \
  --input /opt/spark/work-dir/xy/dase_spark_lab/code/dataset/word2.txt \
  --output /opt/spark/work-dir/xy/dase_spark_lab/code/dataset/wc \
  --partitioner range
"""

import os
import argparse
from datetime import datetime
from pyspark import SparkContext, SparkConf, TaskContext

def main():
    parser = argparse.ArgumentParser(description="WordCount using RDD with Hash/Range partitioning")
    parser.add_argument("--input", required=True, help="输入文件路径")
    parser.add_argument("--output", required=True, help="输出目录路径")
    parser.add_argument(
        "--partitioner",
        choices=["hash", "range"],
        default="hash",
        help="分区策略"
    )
    parser.add_argument(
        "--num-partitions",
        type=int,
        default=4,
        help="分区数量"
    )
    args = parser.parse_args()

    input_path = args.input
    output_base = args.output
    partitioner_type = args.partitioner
    num_partitions = args.num_partitions

    # -----------------------------
    # Spark 配置
    # -----------------------------
    current_time_str = datetime.now().strftime("%m%d%H%M")
    filename_with_ext = os.path.basename(input_path)
    filename = os.path.splitext(filename_with_ext)[0]
    conf = SparkConf().setAppName(f"WordCount-{partitioner_type}-{filename}-{current_time_str}")
    conf.set("spark.eventLog.enabled", "true")
    conf.set("spark.eventLog.dir", "file:///tmp/spark-events")
    conf.set("spark.sql.shuffle.partitions", str(num_partitions))
    sc = SparkContext(conf=conf)

    output_path = f"{output_base}-{current_time_str}"

    # -----------------------------
    # 读取文本文件
    # -----------------------------
    lines = sc.textFile(input_path)
    words = lines.flatMap(lambda line: line.strip().split())
    pairs = words.map(lambda word: (word, 1))

    # -----------------------------
    # 分区策略和第一次 shuffle
    # -----------------------------
    if partitioner_type == "hash":
        # Hash 分区 + map-side combine
        partitioned_rdd = pairs.partitionBy(num_partitions).reduceByKey(lambda a, b: a + b)
    else:
        # Range 分区模拟：先聚合再全局排序
        partitioned_rdd = pairs.reduceByKey(lambda a, b: a + b).sortByKey(num_partitions)

    # -----------------------------
    # 强制每个 executor 只处理一个分区
    # -----------------------------
    # 使用 coalesce 保证分区数量 = executor 数量
    # shuffle=True 确保重新分区触发 shuffle
    partitioned_rdd = partitioned_rdd.coalesce(num_partitions, shuffle=True)

    # -----------------------------
    # 保存每个分区数据（中间结果）
    # -----------------------------
    partitioned_data_path = os.path.join(output_path, "partitioned_data")

    def capture_partition_data(partition):
        tc = TaskContext.get()
        partition_id = tc.partitionId()
        # 每条记录加上 partition 信息
        return [f"Partition {partition_id}: {k}\t{v}" for k, v in partition]

    partitioned_rdd.mapPartitions(capture_partition_data).saveAsTextFile(partitioned_data_path)

    # -----------------------------
    # 全局聚合 WordCount
    # -----------------------------
    final_counts = partitioned_rdd.reduceByKey(lambda a, b: a + b)

    # -----------------------------
    # 保存最终结果为单个文件
    # -----------------------------
    final_output_path = os.path.join(output_path, "final_result")
    final_counts.coalesce(1).map(lambda x: f"{x[0]},{x[1]}").saveAsTextFile(final_output_path)

    print(f"WordCount 完成，结果保存在: {output_path}")
    sc.stop()

if __name__ == "__main__":
    main()
