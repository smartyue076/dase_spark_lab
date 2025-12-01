# -*- coding: utf-8 -*-

import argparse
from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors
from pyspark.ml.clustering import KMeans


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

    # Read raw text data
    df = spark.read.text(args.input)

    # Data format example:
    # 12 4.095992 4.950638 -4.377580 3.632250 1.103754
    # First column is an integer key, others are float features
    df = df.rdd.map(lambda r: r[0].split()).map(
        lambda arr: (int(arr[0]), Vectors.dense([float(x) for x in arr[1:]]))
    ).toDF(["key", "features"])

    # Partitioning strategy
    if args.strategy == "hash":
        df = df.repartition(args.num_partitions, "key")
    elif args.strategy == "range":
        df = df.sortWithinPartitions("key").repartitionByRange(args.num_partitions, "key")

    # KMeans training
    kmeans = (
        KMeans()
        .setK(args.k)
        .setSeed(args.seed)
        .setMaxIter(args.max_iter)
        .setFeaturesCol("features")
    )

    model = kmeans.fit(df)

    print("=== KMeans Result ===")
    print("Cluster Centers:")
    for c in model.clusterCenters():
        print(c)
    print("=====================")

    spark.stop()


if __name__ == "__main__":
    main()
