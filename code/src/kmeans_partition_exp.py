# -*- coding: utf-8 -*-

import argparse
from pyspark.sql import SparkSession
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.clustering import KMeans


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument("--input", required=True, help="path to input txt file")
    parser.add_argument("--strategy", required=True, choices=["hash", "range"])
    parser.add_argument("--num_partitions", type=int, default=4)
    parser.add_argument("--k", type=int, default=8)
    parser.add_argument("--max_iter", type=int, default=20)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--tag", type=str, default="exp")

    return parser.parse_args()


def main():
    args = parse_args()

    spark = (
        SparkSession.builder
        .appName(args.tag)
        .getOrCreate()
    )

    sc = spark.sparkContext

    # =====================================================
    # 1. Load & parse dataset (RDD-based, no numpy required)
    # =====================================================
    raw_rdd = sc.textFile(args.input)

    # Each line example:
    #   12 4.095992 4.950638 -4.377580 3.632250 1.103754
    #
    # We convert to:
    #   key(int), vector(features)
    parsed_rdd = raw_rdd.map(lambda line: line.split()) \
        .map(lambda arr: (int(arr[0]), Vectors.dense([float(x) for x in arr[1:]])))

    # =====================================================
    # 2. Apply partitioning strategy
    # =====================================================
    if args.strategy == "hash":
        # Hash partition on key
        partitioned_rdd = parsed_rdd.partitionBy(args.num_partitions, lambda k: hash(k))
    elif args.strategy == "range":
        # Range partition based on key
        partitioned_rdd = parsed_rdd.sortBy(lambda x: x[0], ascending=True,
                                            numPartitions=args.num_partitions)
    else:
        raise ValueError("Invalid partition strategy.")

    # Only vectors are needed for KMeans.train
    vector_rdd = partitioned_rdd.map(lambda kv: kv[1]).cache()

    # =====================================================
    # 3. Train KMeans (MLLIB version, no numpy needed)
    # =====================================================
    model = KMeans.train(
        vector_rdd,
        k=args.k,
        maxIterations=args.max_iter,
        seed=args.seed
    )

    # =====================================================
    # 4. Output results
    # =====================================================
    print("=== KMeans Result (MLLIB version, no numpy) ===")
    print("Cluster Centers:")
    for c in model.clusterCenters:
        print(c)
    print("==============================================")

    spark.stop()


if __name__ == "__main__":
    main()
