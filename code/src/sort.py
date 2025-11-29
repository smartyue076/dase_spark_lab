#!/usr/bin/env python3
"""
使用纯 RDD 对本地整数文件排序，支持选择分区器：
- range: 全局排序（使用 RangePartitioner）
- hash: 哈希分区 + 分区内排序（使用 HashPartitioner）

输入：每行一个整数的文本文件  
输出：排序后的整数（每行一个），写入单个 part-00000 文件

用法:
/opt/spark/bin/spark-submit \
  --master spark://172.23.166.104:7077 \
  --executor-memory 1G \
  --executor-cores 1 \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=file:///tmp/spark-events \
  /opt/spark/work-dir/code/src/sort.py \
  --input /opt/spark/work-dir/code/dataset/inbalance.txt \
  --output /opt/spark/work-dir/code/dataset/sorted_numbers \
  --partitioner hash
"""

import argparse
import sys
from datetime import datetime
from pyspark import SparkContext, SparkConf


def parse_int(s):
    try:
        return int(s.strip())
    except Exception:
        return None


def sort_partition(iterator):
    """对单个分区内的数据排序"""
    data = list(iterator)
    data.sort()
    return iter(data)


def main():
    parser = argparse.ArgumentParser(description="RDD 排序：支持 hash 或 range 分区器")
    parser.add_argument("--input", required=True, help="输入文件路径（每行一个整数）")
    parser.add_argument("--output", required=True, help="输出目录路径")
    parser.add_argument(
        "--partitioner",
        choices=["range", "hash"],
        default="range",
        help="分区策略: 'range'（全局排序）或 'hash'（分区内排序）"
    )
    parser.add_argument(
        "--num-partitions",
        type=int,
        default=8,
        help="分区数量"
    )
    args = parser.parse_args()

    input_path = args.input
    output_path = args.output
    partitioner_type = args.partitioner
    num_partitions = args.num_partitions

    # 配置 Spark
    current_time_str = datetime.now().strftime("%m%d%H%M")
    conf = SparkConf().setAppName(f"RDD-Sort-{partitioner_type}-{current_time_str}")
    # 启用事件日志（兼容你的 History Server）
    conf.set("spark.eventLog.enabled", "true")
    conf.set("spark.eventLog.dir", "file:///tmp/spark-events")
    conf.set("spark.sql.shuffle.partitions", str(num_partitions))  # 虽然不用 SQL，但影响 shuffle 默认值

    sc = SparkContext(conf=conf)
    sc.setLogLevel("WARN")

    print(f"📂 输入: {input_path}")
    print(f"💾 输出: {output_path}")
    print(f"🧩 分区器: {partitioner_type} (partitions={num_partitions})")

    try:
        # 1. 读取文本并转为整数 RDD，过滤无效行
        lines = sc.textFile(input_path)
        numbers = lines.map(parse_int).filter(lambda x: x is not None)

        if partitioner_type == "range":
            # === 全局排序：使用 sortBy() → 自动用 RangePartitioner ===
            print("🔄 执行全局排序（RangePartitioner）...")
            sorted_rdd = numbers.sortBy(lambda x: x, ascending=True, numPartitions=num_partitions)

        elif partitioner_type == "hash":
            # === 哈希分区 + 分区内排序 ===
            print("🔀 执行哈希分区 + 分区内排序（HashPartitioner）...")

            # 转为 (key, value) 形式以便 partitionBy
            keyed_rdd = numbers.map(lambda x: (x, x))

            # 使用 HashPartitioner 重分区
            repartitioned = keyed_rdd.partitionBy(num_partitions)

            # 提取 value 并在每个分区内排序
            values_only = repartitioned.map(lambda kv: kv[1])
            sorted_rdd = values_only.mapPartitions(sort_partition)

        else:
            raise ValueError(f"未知分区器: {partitioner_type}")

        # 3. 写入结果（强制合并为单个文件）
        print("⏳ 写入结果...")
        output_path = f"{output_path}-{current_time_str}"
        sorted_rdd.coalesce(1).saveAsTextFile(output_path)

        print(f"✅ 完成！结果: {output_path}/part-00000")

    except Exception as e:
        print(f"❌ 错误: {e}", file=sys.stderr)
        raise
    finally:
        sc.stop()


if __name__ == "__main__":
    main()