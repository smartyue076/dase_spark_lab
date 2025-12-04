#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
from collections import Counter
import numpy as np

def analyze_zipf_file(file_path, top_n=20):
    """
    读取文件，分析 Zipf 分布，输出 top_n 单词及其频率
    并估算 Zipf s 参数
    """
    # 读取文件
    with open(file_path, "r", encoding="utf-8") as f:
        words = [line.strip() for line in f if line.strip()]

    # 统计每个单词出现次数
    counter = Counter(words)
    total_count = sum(counter.values())

    # 按频率排序
    sorted_items = counter.most_common()

    print(f"总单词数: {total_count}, 不同单词数: {len(counter)}\n")
    print(f"Top-{top_n} 单词及频率:")
    print(f"{'rank':<5}{'word':<12}{'count':<10}{'freq':<10}")
    for i, (word, count) in enumerate(sorted_items[:top_n], start=1):
        print(f"{i:<5}{word:<12}{count:<10}{count/total_count:<10.4f}")

    # -----------------------------
    # 使用线性回归估算 Zipf s 参数
    # Zipf law: freq ~ 1 / rank^s => log(freq) = -s*log(rank) + C
    # -----------------------------
    ranks = np.arange(1, len(sorted_items)+1)
    freqs = np.array([count for _, count in sorted_items])

    log_ranks = np.log(ranks)
    log_freqs = np.log(freqs)

    # 线性拟合 log-log
    coeffs = np.polyfit(log_ranks, log_freqs, 1)
    estimated_s = -coeffs[0]  # 斜率的负数就是 Zipf s

    print(f"\n估算 Zipf s ≈ {estimated_s:.4f}")

def main():
    parser = argparse.ArgumentParser(description="Analyze Zipf-distributed file")
    parser.add_argument("--input_file", type=str, required=True, help="Zipf数据文件，每行一个单词")
    parser.add_argument("--top_n", type=int, default=20, help="输出Top-N单词频率")
    args = parser.parse_args()

    analyze_zipf_file(args.input_file, args.top_n)

if __name__ == "__main__":
    main()
