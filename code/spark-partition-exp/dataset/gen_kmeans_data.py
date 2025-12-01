import numpy as np
import argparse
from collections import Counter
from math import log2
import os
import csv


def compute_skew_metrics(keys, n_keys):
    cnt = Counter(keys)
    counts = np.array([cnt.get(i, 0) for i in range(n_keys)])

    min_count = int(counts.min())
    max_count = int(counts.max())
    mean = float(counts.mean())
    std = float(counts.std())
    cv = std / mean if mean > 0 else 0.0       # 变异系数
    max_mean_ratio = max_count / mean if mean > 0 else 0.0

    p = counts / counts.sum()
    entropy = -np.sum(p * np.log2(p + 1e-12))
    norm_entropy = entropy / log2(n_keys) if n_keys > 1 else 1.0

    return {
        "min_count": min_count,
        "max_count": max_count,
        "mean": mean,
        "std": std,
        "cv": cv,
        "max_mean_ratio": max_mean_ratio,
        "norm_entropy": norm_entropy
    }


def save_skew_info(path, name, n_keys, metrics):
    file_exists = os.path.exists(path)
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow([
                "name", "n_keys", "min_count", "max_count", "mean",
                "std", "cv", "max_mean_ratio", "norm_entropy"
            ])
        writer.writerow([
            name, n_keys,
            metrics["min_count"],
            metrics["max_count"],
            metrics["mean"],
            metrics["std"],
            metrics["cv"],
            metrics["max_mean_ratio"],
            metrics["norm_entropy"],
        ])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--n_samples", type=int, default=50000)  # 增加数据量至50,000个样本
    parser.add_argument("--n_keys", type=int, default=50)  # 增加key的数量
    parser.add_argument("--dim", type=int, default=5)
    parser.add_argument("--hot_ratio", type=float, default=0.95)  # 增加倾斜度（95%的数据都属于一个key）
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--out_dir", type=str, default=".")
    args = parser.parse_args()

    np.random.seed(args.seed)

    os.makedirs(args.out_dir, exist_ok=True)
    skew_info_path = os.path.join(args.out_dir, "skew_info.csv")

    # ========= 1. 生成 balanced 数据 =========
    base = args.n_samples // args.n_keys
    rem = args.n_samples % args.n_keys
    keys_bal = []
    for k in range(args.n_keys):
        cnt = base + (1 if k < rem else 0)
        keys_bal.extend([k] * cnt)
    keys_bal = np.array(keys_bal)
    np.random.shuffle(keys_bal)

    centers_bal = np.random.randn(args.n_keys, args.dim) * 5.0
    X_bal = np.zeros((args.n_samples, args.dim))
    for i, k in enumerate(keys_bal):
        X_bal[i] = centers_bal[k] + np.random.randn(args.dim)

    balanced_path = os.path.join(args.out_dir, "kmeans-balanced.txt")
    with open(balanced_path, "w", encoding="utf-8") as f:
        for k, feat in zip(keys_bal, X_bal):
            f.write(str(int(k)) + " " + " ".join(f"{x:.6f}" for x in feat) + "\n")

    metrics_bal = compute_skew_metrics(keys_bal, args.n_keys)
    print("===== BALANCED 数据倾斜指标 =====")
    print(metrics_bal)
    save_skew_info(skew_info_path, "balanced", args.n_keys, metrics_bal)

    # ========= 2. 生成 skewed 数据 =========
    n_hot = int(args.n_samples * args.hot_ratio)
    n_other = args.n_samples - n_hot

    keys_skew = [0] * n_hot  # key=0 作为热点
    other_keys = np.random.randint(1, args.n_keys, size=n_other)
    keys_skew.extend(other_keys.tolist())
    keys_skew = np.array(keys_skew)
    np.random.shuffle(keys_skew)

    centers_skew = np.random.randn(args.n_keys, args.dim) * 5.0
    X_skew = np.zeros((args.n_samples, args.dim))
    for i, k in enumerate(keys_skew):
        X_skew[i] = centers_skew[k] + np.random.randn(args.dim)

    skewed_path = os.path.join(args.out_dir, "kmeans-skewed.txt")
    with open(skewed_path, "w", encoding="utf-8") as f:
        for k, feat in zip(keys_skew, X_skew):
            f.write(str(int(k)) + " " + " ".join(f"{x:.6f}" for x in feat) + "\n")

    metrics_skew = compute_skew_metrics(keys_skew, args.n_keys)
    print("===== SKEWED 数据倾斜指标 =====")
    print(metrics_skew)
    save_skew_info(skew_info_path, "skewed", args.n_keys, metrics_skew)

    print("数据生成完成，文件保存在：", args.out_dir)


if __name__ == "__main__":
    main()
