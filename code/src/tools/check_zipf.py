#!/usr/bin/env python3
import argparse
import numpy as np
from collections import Counter

def load_numbers(path):
    """读取文件中的 key（每行一个字符串数字）"""
    with open(path) as f:
        return [line.strip() for line in f if line.strip()]

def linear_regression(x, y):
    """纯 numpy 线性回归，返回 slope, intercept, r2"""
    x = np.array(x)
    y = np.array(y)

    x_mean = x.mean()
    y_mean = y.mean()

    slope = np.sum((x - x_mean)*(y - y_mean)) / np.sum((x - x_mean)**2)
    intercept = y_mean - slope * x_mean

    y_pred = slope * x + intercept
    ss_tot = np.sum((y - y_mean)**2)
    ss_res = np.sum((y - y_pred)**2)
    r2 = 1 - ss_res/ss_tot

    return slope, intercept, r2

def is_zipf(file_path, min_r2=0.95, top_n=5):
    """判断文件是否符合 Zipf 分布，并输出热点数据占比和 top key"""
    nums = load_numbers(file_path)
    total_count = len(nums)
    freq = Counter(nums)

    # 获取 top N 的 (key, count)
    top_items = freq.most_common(top_n)
    top_keys = [k for k, v in top_items]
    top_freqs = [v for k, v in top_items]

    freqs_sorted = np.array(sorted(freq.values(), reverse=True))
    ranks = np.arange(1, len(freqs_sorted) + 1)

    log_ranks = np.log(ranks)
    log_freqs = np.log(freqs_sorted)

    slope, intercept, r2 = linear_regression(log_ranks, log_freqs)

    print("===== Zipf 拟合结果（纯 numpy）=====")
    print(f"拟合参数 s（Zipf 指数） = {-slope:.4f}")
    print(f"R² = {r2:.4f}")
    print(f"数据总量 = {total_count}")
    print(f"唯一 key 数 = {len(freq)}")

    # 🔥 热点数据占比
    top_sum = sum(top_freqs)
    ratio = top_sum / total_count

    print(f"前 {top_n} 个最频繁 key: {top_keys}")
    print(f"对应出现次数: {top_freqs}")
    print(f"占总数据比例: {ratio:.4%}")
    print("===================================")

    if r2 >= min_r2:
        print("✔ 结论：非常符合 Zipf 分布")
        return True
    else:
        print("✘ 结论：不符合 Zipf 分布")
        return False
    
def main():
    parser = argparse.ArgumentParser(description="判断文件是否符合 Zipf 分布，并输出热点数据比例")
    parser.add_argument("--input", required=True, help="输入文件路径")
    parser.add_argument("--r2", type=float, default=0.95, help="R² 判定阈值（默认0.95）")
    parser.add_argument("--top-n", type=int, default=5, help="计算前N个热点key的比例")
    args = parser.parse_args()

    print(f"正在分析文件: {args.input}")
    print(f"Zipf 判定 R² 阈值: {args.r2}")
    print(f"统计前 {args.top_n} 个热点 key 占比")

    is_zipf(args.input, min_r2=args.r2, top_n=args.top_n)

if __name__ == "__main__":
    main()
