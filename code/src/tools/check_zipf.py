#!/usr/bin/env python3
import argparse
import numpy as np
from collections import Counter


def load_numbers(path):
    """读取文件中的 key（每行一个字符串数字）"""
    with open(path) as f:
        return [line.strip() for line in f if line.strip()]


def linear_regression(x, y):
    """
    纯 numpy 版本线性回归，返回 slope, intercept, r2
    """
    x = np.array(x)
    y = np.array(y)

    x_mean = x.mean()
    y_mean = y.mean()

    slope = np.sum((x - x_mean) * (y - y_mean)) / np.sum((x - x_mean)**2)
    intercept = y_mean - slope * x_mean

    y_pred = slope * x + intercept

    ss_tot = np.sum((y - y_mean)**2)
    ss_res = np.sum((y - y_pred)**2)

    r2 = 1 - ss_res / ss_tot

    return slope, intercept, r2


def is_zipf(file_path, min_r2=0.95):
    """判断一个文件是否符合 Zipf 分布"""
    nums = load_numbers(file_path)

    freq = Counter(nums)
    freqs = np.array(sorted(freq.values(), reverse=True))
    ranks = np.arange(1, len(freqs) + 1)

    # log(rank) 和 log(freq)
    log_ranks = np.log(ranks)
    log_freqs = np.log(freqs)

    slope, intercept, r2 = linear_regression(log_ranks, log_freqs)

    print("===== Zipf 拟合结果（纯 numpy）=====")
    print(f"Zipf 指数 s（= -slope）= {-slope:.4f}")
    print(f"R² = {r2:.4f}")
    print(f"数据总量 = {len(nums)}")
    print(f"唯一 key 数 = {len(freqs)}")
    print("===================================")

    if r2 >= min_r2:
        print("✔ 结论：非常符合 Zipf 分布")
        return True
    else:
        print("✘ 结论：不符合 Zipf 分布")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="判断文件是否符合 Zipf 分布（每行一个数字）"
    )
    parser.add_argument("--input", required=True, help="输入文件路径")
    parser.add_argument("--r2", type=float, default=0.95,
                        help="R² 判定阈值（默认 0.95）")

    args = parser.parse_args()

    print(f"正在分析文件: {args.input}")
    print(f"Zipf 判定 R² 阈值: {args.r2}")

    is_zipf(args.input, args.r2)


if __name__ == "__main__":
    main()
