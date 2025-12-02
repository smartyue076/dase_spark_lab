#!/usr/bin/env python3
"""
检查文件中的数列是否递增有序（支持固定宽度的数字字符串，例如 000123）
文件格式：每行一个数字（可能带前导 0）
"""

import sys

def is_sorted(file_path):
    prev = None
    line_num = 0

    with open(file_path, "r") as f:
        for line in f:
            line_num += 1
            line = line.strip()
            if not line:
                continue

            try:
                # 固定宽度数字 → 必须按整数解析，否则会按字典序错误比较
                num = int(line)
            except ValueError:
                print(f"❌ 第 {line_num} 行无法解析为数字: {line}")
                return False

            if prev is not None and num < prev:
                print(f"❌ 序列未递增: 第 {line_num-1} 行 {prev} > 第 {line_num} 行 {num}")
                return False

            prev = num

    return True


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("用法: python check_sorted.py /path/to/file.txt")
        sys.exit(1)

    file_path = sys.argv[1]
    if is_sorted(file_path):
        print("✅ 文件中的序列是递增有序的")
    else:
        print("❌ 文件中的序列不是递增有序的")
