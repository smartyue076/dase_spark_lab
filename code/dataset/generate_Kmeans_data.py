# -*- coding: utf-8 -*-

import numpy as np
import argparse
from collections import Counter
from math import log2
import os
import csv
import random
import matplotlib.pyplot as plt


# ==============================
#   Zipf 分布生成 key 序列
# ==============================
def generate_zipf_keys(n, n_keys, s):
    """
    n       : 样本数量
    n_keys  : key 的范围 [0, n_keys-1]
    s       : Zipf 参数，s=0为均匀
    """
    if s < 0:
        raise ValueError("s must >= 0")

    keys = np.arange(n_keys)

    if s == 0:
        return np.random.choice(keys, size=n)

    # Zipf 权重
    probs = np.array([1 / ((k + 1) ** s) for k in keys], dtype=float)
    probs /= probs.sum()

    return np.random.choice(keys, size=n, p=probs)


# ==============================
#   计算数据倾斜指标
# ==============================
def compute_skew_metrics(keys, n_keys):
    cnt = Counter(keys)
    counts = np.array([cnt.get(i, 0) for i in range(n_keys)])

    min_count = int(counts.min())
    max_count = int(counts.max())
    mean = float(counts.mean())
    std = float(counts.std())
    cv = std / mean if mean > 0 else 0.0
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


# ==============================
#   保存 skew 指标
# ==============================
def save_skew_info(path, name, n_keys, metrics, seed):
    file_exists = os.path.exists(path)
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow([
                "name", "n_keys", "seed",
                "min_count", "max_count", "mean",
                "std", "cv", "max_mean_ratio", "norm_entropy"
            ])
        writer.writerow([
            name, n_keys, seed,
            metrics["min_count"],
            metrics["max_count"],
            metrics["mean"],
            metrics["std"],
            metrics["cv"],
            metrics["max_mean_ratio"],
            metrics["norm_entropy"],
        ])


# ==============================
#   绘图：展示 Zipf key 分布
# ==============================
def plot_zipf_distribution(keys, name, out_dir):
    cnt = Counter(keys)
    keys_sorted = sorted(cnt.keys())
    values = [cnt[k] for k in keys_sorted]

    plt.figure(figsize=(10, 5))
    plt.bar(keys_sorted, values, color="steelblue")
    plt.xlabel("Key")
    plt.ylabel("Frequency")
    plt.title(f"Zipf Distribution: {name}")
    plt.tight_layout()

    out_path = os.path.join(out_dir, f"zipf_{name}.png")
    plt.savefig(out_path)
    plt.close()

    print(f"📈 Saved plot: {out_path}")


# ==============================
#          主函数
# ==============================
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--n_samples", type=int, default=10000000)
    parser.add_argument("--n_keys", type=int, default=50)
    parser.add_argument("--dim", type=int, default=5)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--out_dir", type=str, default=".")
    args = parser.parse_args()

    # === 固定随机种子（可复现） ===
    np.random.seed(args.seed)
    random.seed(args.seed)

    os.makedirs(args.out_dir, exist_ok=True)
    skew_info_path = os.path.join(args.out_dir, "skew_info.csv")

    zipf_settings = [
        ("s0_uniform", 0),
        ("s0.5_medium", 0.5),
        ("s1_heavy", 1),
    ]

    for name, s in zipf_settings:
        print(f"\n===== 生成 {name} (s={s}) 数据 =====")

        # 1. 生成 Zipf key
        keys = generate_zipf_keys(
            n=args.n_samples,
            n_keys=args.n_keys,
            s=s
        )

        # 2. 生成 KMeans 特征
        centers = np.random.randn(args.n_keys, args.dim) * 5.0
        X = np.zeros((args.n_samples, args.dim))
        for i, k in enumerate(keys):
            X[i] = centers[k] + np.random.randn(args.dim)

        # 3. 写文件
        out_path = os.path.join(args.out_dir, f"kmeans-{name}.txt")
        with open(out_path, "w", encoding="utf-8") as f:
            for k, feat in zip(keys, X):
                f.write(str(int(k)) + " " + " ".join(f"{x:.6f}" for x in feat) + "\n")

        # 4. 倾斜度指标
        metrics = compute_skew_metrics(keys, args.n_keys)
        print(metrics)
        save_skew_info(skew_info_path, name, args.n_keys, metrics, args.seed)

        # 5. 绘制 Zipf 分布图
        plot_zipf_distribution(keys, name, args.out_dir)

    print("\n📌 所有 Zipf 数据生成完毕，保存目录：", args.out_dir)


if __name__ == "__main__":
    main()
