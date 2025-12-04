#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import numpy as np

def generate_zipf_sequence(n, s, vocab_size=1000, random_seed=43):
    """
    生成长度为 n 的 Zipf 分布序列
    - n: 序列长度
    - s: Zipf 分布的偏斜参数 (s>=0)
    - vocab_size: 词汇表大小
    - random_seed: 固定随机种子保证序列内容一致
    """
    np.random.seed(random_seed)

    # 构建词汇表
    word_length = 10
    vocab = [f"w{str(i).zfill(word_length - 1)}" for i in range(1, vocab_size+1)]

    if s == 0:
        # s=0 表示均匀分布
        probs = np.ones(vocab_size) / vocab_size
    else:
        ranks = np.arange(1, vocab_size+1)
        probs = 1 / np.power(ranks, s)
        probs = probs / probs.sum()  # 归一化

    # 生成长度为 n 的序列
    sequence = np.random.choice(vocab, size=n, p=probs)
    return sequence

def main():
    parser = argparse.ArgumentParser(description="Generate Zipf-distributed sequence")
    parser.add_argument("--n", type=int, default=10, help="Sequence size")
    parser.add_argument("--s", type=float, default=1.0, help="Skew parameter (s>=0), s=0 means uniform distribution")
    parser.add_argument("--vocab_size", type=int, default=100000, help="Vocabulary size")
    parser.add_argument("--o", type=str, default="sequence.txt", help="Output file path")
    args = parser.parse_args()

    sequence = generate_zipf_sequence(args.n, args.s, args.vocab_size)

    # 写入文件，每行一个单词
    with open(args.o, "w", encoding="utf-8") as f:
        for word in sequence:
            f.write(f"{word}\n")

    print(f"数据集生成完成: {args.o} (长度={args.n}, s={args.s})")

if __name__ == "__main__":
    main()
