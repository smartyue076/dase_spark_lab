#!/usr/bin/env python3
"""
生成指定数量的随机整数（范围：-1_000_000 到 1_000_000），支持指定比例的重复值。
每行一个数字，无额外格式。

用法:
生成不均匀数据（90%为同一个值）
    python generate_numbers.py --num 100000000 --output inbalance.txt --repeat-ratio 0.9

生成均匀数据（完全随机）
    python generate_numbers.py --num 100000000 --output balance.txt --repeat-ratio 0.0
"""

import argparse
import random
import sys


def generate_ints_with_repeats(num_rows, repeat_ratio, low=-1_000_000, high=1_000_000):
    """
    生成包含重复值的整数序列。
    
    Args:
        num_rows: 总行数
        repeat_ratio: 重复值所占比例（0.0 ~ 1.0）
        low, high: 随机数范围（含）
    
    Yields:
        str: 每行一个数字加换行符
    """
    if not (0.0 <= repeat_ratio <= 1.0):
        raise ValueError("repeat_ratio must be between 0.0 and 1.0")

    num_repeat = int(num_rows * repeat_ratio)
    num_unique = num_rows - num_repeat

    # 生成唯一值部分
    unique_values = [str(random.randint(low, high)) for _ in range(num_unique)]

    # 生成重复值：选一个固定值作为重复项
    repeat_value = str(random.randint(low, high))
    repeat_values = [repeat_value] * num_repeat

    # 合并并打乱顺序
    all_values = unique_values + repeat_values
    random.shuffle(all_values)

    for value in all_values:
        yield value + "\n"


def main():
    parser = argparse.ArgumentParser(description="生成带可控重复比例的随机整数文件")
    parser.add_argument("--num", type=int, default=10000000, help="要生成的数字数量（例如：10000000）")
    parser.add_argument("--output", type=str, default="random_numbers.txt", help="输出文件路径")
    parser.add_argument("--repeat-ratio", type=float, default=0.0, help="重复值比例（0.0=无重复，1.0=全相同）")
    parser.add_argument("--seed", type=int, default=42, help="随机种子（默认：42）")
    args = parser.parse_args()

    # 设置随机种子以确保可重现
    random.seed(args.seed)

    num_rows = args.num
    output_file = args.output
    repeat_ratio = args.repeat_ratio

    print(f"正在生成 {num_rows:,} 个整数（范围: -1,000,000 ~ 1,000,000）...")
    print(f"重复值比例: {repeat_ratio:.2%}")
    print(f"随机种子: {args.seed}")
    print(f"输出文件: {output_file}")

    try:
        with open(output_file, "w", buffering=1024*1024) as f:
            f.writelines(generate_ints_with_repeats(num_rows, repeat_ratio))
        print("生成完成！")
    except KeyboardInterrupt:
        print("\n用户中断，已退出。")
        sys.exit(1)
    except Exception as e:
        print(f"错误: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()