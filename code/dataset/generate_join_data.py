# """
# 用法：
# python3 generate_join_data.py \
#     --num 100000 \
#     --repeatA 0.0 \
#     --repeatB 0.8 \
#     --outputA dataset/tableA.csv \
#     --outputB dataset/tableB.csv \
#     --seedA 42 \
#     --seedB 43
# """
# import argparse
# import random
# import sys


# def generate_pairs_with_repeats(num_rows, repeat_ratio, low=-1_000_000, high=1_000_000):
#     """
#     生成 key-value 结构，key 包含按比例重复值，用于 Spark Join。
#     每行输出： key,value
#     """
#     if not (0.0 <= repeat_ratio <= 1.0):
#         raise ValueError("repeat_ratio must be between 0.0 and 1.0")

#     num_repeat = int(num_rows * repeat_ratio)
#     num_unique = num_rows - num_repeat

#     # 生成唯一 key
#     unique_keys = [str(random.randint(low, high)) for _ in range(num_unique)]

#     # 生成重复 key（热点 key）
#     repeat_key = str(random.randint(low, high))
#     repeat_keys = [repeat_key] * num_repeat

#     # 合并 key
#     all_keys = unique_keys + repeat_keys
#     random.shuffle(all_keys)

#     # 生成 value
#     for key in all_keys:
#         value = random.randint(1, 1000000)
#         yield f"{key},{value}\n"


# def write_file(filename, generator_fn):
#     """统一的带缓冲写入方式"""
#     with open(filename, "w", buffering=1024 * 1024) as f:
#         for line in generator_fn:
#             f.write(line)


# def main():
#     parser = argparse.ArgumentParser(description="生成 Spark Join 用的两个数据集（可复现、可控倾斜）")

#     parser.add_argument("--num", type=int, default=10_000_000, help="每个数据集的记录数")
#     parser.add_argument("--outputA", type=str, default="dataset/tableA.csv", help="输出文件 A")
#     parser.add_argument("--outputB", type=str, default="dataset/tableB.csv", help="输出文件 B")

#     parser.add_argument("--repeatA", type=float, default=0.0, help="A 的重复 key 比例")
#     parser.add_argument("--repeatB", type=float, default=0.0, help="B 的重复 key 比例")

#     parser.add_argument("--seedA", type=int, default=42, help="随机种子 A")
#     parser.add_argument("--seedB", type=int, default=43, help="随机种子 B")

#     args = parser.parse_args()

#     print("=== Spark Join 数据生成 ===")
#     print(f"A 总行数: {args.num:,}, 重复比例: {args.repeatA:.2%}, 种子: {args.seedA}")
#     print(f"B 总行数: {args.num:,}, 重复比例: {args.repeatB:.2%}, 种子: {args.seedB}")
#     print()

#     # ---------- 生成 A ----------
#     print(f"[A] 正在生成 {args.outputA} ...")
#     random.seed(args.seedA)
#     write_file(
#         args.outputA,
#         generate_pairs_with_repeats(args.num, args.repeatA),
#     )
#     print("[A] 完成\n")

#     # ---------- 生成 B ----------
#     print(f"[B] 正在生成 {args.outputB} ...")
#     random.seed(args.seedB)
#     write_file(
#         args.outputB,
#         generate_pairs_with_repeats(args.num, args.repeatB),
#     )
#     print("[B] 完成\n")

#     print("生成结束！两个数据集已可用于 Spark Join 实验。")


# if __name__ == "__main__":
#     try:
#         main()
#     except KeyboardInterrupt:
#         print("\n用户中断。")
#         sys.exit(1)
#     except Exception as e:
#         print(f"错误: {e}")
#         sys.exit(1)


"""
用法：
python3 generate_join_data.py \
    --num 100000 \
    --sA 0 \
    --sB 0 \
    --max_key 1000000 \
    --outputA dataset/tableC.csv \
    --outputB dataset/tableD.csv \
    --seedA 42 \
    --seedB 43
"""

import argparse
import random
import sys


def generate_zipf_keys(num_rows, s, max_key=1_000_000):
    """
    返回: 一个 key 的字符串序列，每个 key 是 1 ~ max_key 范围内的字符串。
    """

    if s < 0:
        raise ValueError("Zipf 参数 s 必须 >= 0（s=0 等价于均匀分布）")

    # key 候选列表
    keys = list(range(1, max_key + 1))

    # --- 计算 Zipf 权重 ---
    if s == 0:
        # 均匀分布
        weights = None
    else:
        weights = [1 / (k ** s) for k in keys]
        total = sum(weights)
        weights = [w / total for w in weights]  # 归一化

    # --- 采样 ---
    sampled_keys = random.choices(keys, weights=weights, k=num_rows)

    # 字符串化
    for k in sampled_keys:
        yield str(k)


def generate_pairs_zipf(num_rows, s, max_key=1_000_000):
    """
    返回 "key,value" 字符串，用于写入 CSV
    """
    for key in generate_zipf_keys(num_rows, s, max_key=max_key):
        value = random.randint(1, 1_000_000)
        yield f"{key},{value}\n"


def write_file(filename, generator_fn):
    """统一的高速写入"""
    with open(filename, "w", buffering=1024 * 1024) as f:
        for line in generator_fn:
            f.write(line)


def main():
    parser = argparse.ArgumentParser(description="基于 Zipf 分布生成 Spark Join 实验数据")

    parser.add_argument("--num", type=int, default=10_000_000, help="A/B 每个表的记录数")
    parser.add_argument("--max_key", type=int, default=1_000_000, help="Zipf 分布 key 的最大值")

    parser.add_argument("--sA", type=float, default=0.0, help="表 A 的 Zipf 倾斜参数")
    parser.add_argument("--sB", type=float, default=0.0, help="表 B 的 Zipf 倾斜参数")

    parser.add_argument("--seedA", type=int, default=42)
    parser.add_argument("--seedB", type=int, default=43)

    parser.add_argument("--outputA", type=str, default="dataset/tableA.csv")
    parser.add_argument("--outputB", type=str, default="dataset/tableB.csv")

    args = parser.parse_args()

    print("=== Spark Join 数据生成 ===")
    print(f"A: num={args.num:,}, s={args.sA}, seed={args.seedA}")
    print(f"B: num={args.num:,}, s={args.sB}, seed={args.seedB}")
    print(f"max_key = {args.max_key:,}")
    print()

    # --- A ---
    print(f"[A] 生成中: {args.outputA}")
    random.seed(args.seedA)
    write_file(
        args.outputA,
        generate_pairs_zipf(args.num, args.sA, args.max_key)
    )
    print("[A] 完成\n")

    # --- B ---
    print(f"[B] 生成中: {args.outputB}")
    random.seed(args.seedB)
    write_file(
        args.outputB,
        generate_pairs_zipf(args.num, args.sB, args.max_key)
    )
    print("[B] 完成\n")

    print("数据已可以用于 Spark Join 倾斜实验")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n中断退出")
        sys.exit(1)
    except Exception as e:
        print(f"错误: {e}")
        sys.exit(1)
