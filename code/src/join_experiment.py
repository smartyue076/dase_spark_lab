#!/usr/bin/env python3
"""
Spark RDD Join 实验脚本
支持分区器选择：
- hash: HashPartitioner
- range: RangePartitioner（需要 key 可排序）
- custom: 自定义分区器（简单示例：奇偶 key 分区）

输入：
- 每行 "key,value" 的 CSV 文件
输出：
- Join 结果写入单个 part-00000 文件
"""

"""
/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --executor-memory 1G \
  --executor-cores 1 \
  /opt/spark/work-dir/join_experiment.py \
  --inputA /opt/spark/work-dir/dataset/tableA.csv \
  --inputB /opt/spark/work-dir/dataset/tableB.csv \
  --output /opt/spark/work-dir/dataset/join_result \
  --partitioner hash \
  --num-partitions 8
"""


import argparse
import sys
import time
from datetime import datetime
from pyspark import SparkContext, SparkConf
from pyspark.rdd import portable_hash

# -----------------------------
# 自定义分区器示例
# -----------------------------
class CustomPartitioner:
    """简单自定义分区器：奇偶 key 分到不同分区"""
    def __init__(self, num_partitions):
        self.num_partitions = num_partitions

    def __call__(self, key):
        try:
            return int(key) % self.num_partitions
        except Exception:
            return 0  # 出错 key 放到 0 分区

    def numPartitions(self):
        return self.num_partitions

# -----------------------------
# 工具函数
# -----------------------------
def parse_kv(line):
    try:
        key, value = line.strip().split(",", 1)
        return key, value
    except Exception:
        return None

# -----------------------------
# 主函数
# -----------------------------
def main():
    parser = argparse.ArgumentParser(description="Spark RDD Join 实验")
    parser.add_argument("--inputA", required=True, help="输入文件 A (key,value)")
    parser.add_argument("--inputB", required=True, help="输入文件 B (key,value)")
    parser.add_argument("--output", required=True, help="输出目录")
    parser.add_argument(
        "--partitioner",
        choices=["hash", "range", "custom"],
        default="hash",
        help="分区策略"
    )
    parser.add_argument("--num-partitions", type=int, default=8, help="分区数量")
    args = parser.parse_args()

    conf = SparkConf().setAppName(f"RDD-Join-{args.partitioner}-{datetime.now().strftime('%m%d%H%M')}")
    conf.set("spark.eventLog.enabled", "true")
    conf.set("spark.eventLog.dir", "file:///tmp/spark-events")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("WARN")

    print(f"📂 输入A: {args.inputA}")
    print(f"📂 输入B: {args.inputB}")
    print(f"🧩 分区器: {args.partitioner}, 分区数={args.num_partitions}")

    start_time = time.time()
    try:
        # 读取 CSV
        rddA = sc.textFile(args.inputA).map(parse_kv).filter(lambda x: x is not None)
        rddB = sc.textFile(args.inputB).map(parse_kv).filter(lambda x: x is not None)

        if args.partitioner == "hash":
            print("🔀 使用 HashPartitioner")
            rddA = rddA.partitionBy(args.num_partitions)
            rddB = rddB.partitionBy(args.num_partitions)

        elif args.partitioner == "range":
            print("🔄 使用 RangePartitioner (通过 sortByKey 实现)")
            rddA = rddA.sortByKey(ascending=True, numPartitions=args.num_partitions)
            rddB = rddB.sortByKey(ascending=True, numPartitions=args.num_partitions)

        elif args.partitioner == "custom":
            print("✨ 使用自定义分区器")
            part = CustomPartitioner(args.num_partitions)
            rddA = rddA.partitionBy(args.num_partitions, partitionFunc=part)
            rddB = rddB.partitionBy(args.num_partitions, partitionFunc=part)

        else:
            raise ValueError(f"未知分区器: {args.partitioner}")

        # 执行 Join
        print("⏳ 执行 Join ...")
        joined = rddA.join(rddB)

        # 写入结果到单个文件
        output_path = f"{args.output}-{int(time.time())}"
        joined.coalesce(1).saveAsTextFile(output_path)

        elapsed = time.time() - start_time
        print(f"✅ Join 完成! 输出: {output_path}/part-00000")
        print(f"⏱ 总耗时: {elapsed:.2f}s")
        print(f"♻️ 数据量: {joined.count()} 条记录")

    except Exception as e:
        print(f"❌ 错误: {e}", file=sys.stderr)
        raise
    finally:
        sc.stop()

if __name__ == "__main__":
    main()
