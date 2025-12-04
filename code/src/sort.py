#!/usr/bin/env python3
"""
使用纯 RDD 对本地整数文件排序，支持选择分区器：
- range: 全局排序（使用 RangePartitioner）
- hash: 哈希分区 + 分区内排序，但最终输出全局有序

输入：每行一个整数的文本文件  
输出：排序后的整数（每行一个），写入单个 part-00000 文件

用法:
/opt/spark/bin/spark-submit \
  --master spark://172.23.166.104:7077 \
  --executor-memory 1G \
  --executor-cores 1 \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=file:///tmp/spark-events \
  /opt/spark/work-dir/xy/dase_spark_lab/code/src/sort.py \
  --input /opt/spark/work-dir/xy/dase_spark_lab/code/dataset/heavy_skew.txt \
  --output /opt/spark/work-dir/xy/dase_spark_lab/code/dataset/sorted_numbers \
  --partitioner range
"""

import argparse
import sys
import os
from datetime import datetime
from pyspark import SparkContext, SparkConf
from pyspark.rdd import portable_hash

def parse_key(s):
    v = s.strip()
    return v if v else None

def sort_partition(iterator):
    data = list(iterator)
    data.sort()
    return iter(data)

def main():
    parser = argparse.ArgumentParser(description="RDD 排序并导出每分区数据")
    parser.add_argument("--input", required=True, help="输入文件路径（每行一个整数）")
    parser.add_argument("--output", required=True, help="输出目录路径")
    parser.add_argument(
        "--partitioner",
        choices=["range", "hash"],
        default="range",
        help="分区策略: range 或 hash"
    )
    parser.add_argument(
        "--num-partitions",
        type=int,
        default=4,
        help="分区数量"
    )
    args = parser.parse_args()

    input_path = args.input
    output_path = args.output
    partitioner_type = args.partitioner
    num_partitions = args.num_partitions

    # 配置 Spark
    current_time_str = datetime.now().strftime("%m%d%H%M")
    filename_with_ext = os.path.basename(input_path)
    filename = os.path.splitext(filename_with_ext)[0]
    conf = SparkConf().setAppName(f"A-Sort-{partitioner_type}-{filename}-{current_time_str}")
    conf.set("spark.eventLog.enabled", "true")
    conf.set("spark.eventLog.dir", "file:///tmp/spark-events")
    conf.set("spark.sql.shuffle.partitions", str(num_partitions))

    output_path = f"{output_path}-{current_time_str}"
    partition_dump_path = f"{output_path}-partitions"

    sc = SparkContext(conf=conf)
    sc.setLogLevel("WARN")

    print(f"📂 输入: {input_path}")
    print(f"💾 输出: {output_path}")
    print(f"📁 分区数据 dump: {partition_dump_path}")
    print(f"🧩 分区器: {partitioner_type} (numPartitions={num_partitions})")

    try:
        # 1. 加载数据
        lines = sc.textFile(input_path)
        numbers = lines.map(parse_key).filter(lambda x: x is not None)

        if partitioner_type == "range":
            print("🔄 执行全局排序（RangePartitioner）...")
            sorted_rdd = numbers.sortBy(lambda x: x, ascending=True, numPartitions=num_partitions)

        elif partitioner_type == "hash":
            print("🔀 执行哈希分区 + 分区内排序 + 全局排序...")
            keyed_rdd = numbers.map(lambda x: (x, x))
            repartitioned = keyed_rdd.partitionBy(num_partitions, partitionFunc=portable_hash)
            locally_sorted = repartitioned.map(lambda kv: kv[1]).mapPartitions(sort_partition)
            sorted_rdd = locally_sorted.sortBy(lambda x: x, ascending=True, numPartitions=num_partitions)

        else:
            raise ValueError(f"未知分区器类型: {partitioner_type}")

        # # 2. 保存每分区的数据（关键部分！）
        # print("📝 保存每个分区的数据...")
        # partition_dump_rdd = sorted_rdd.mapPartitionsWithIndex(
        #     lambda it: (f"{v}" for v in it)
        # )
        # partition_dump_rdd.saveAsTextFile(partition_dump_path)

        # 3. 保存最终排序结果
        print("⏳ 写入最终排序输出...")
        sorted_rdd.coalesce(1).saveAsTextFile(output_path)

        print(f"✅ 完成！结果：{output_path}/part-00000")
        # print(f"📁 分区 dump：{partition_dump_path}")

    except Exception as e:
        print(f"❌ 错误: {e}", file=sys.stderr)
        raise
    finally:
        sc.stop()

if __name__ == "__main__":
    main()