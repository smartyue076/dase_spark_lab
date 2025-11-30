"""
用法：
python3 generate_join_data.py \
    --num 100000 \
    --repeatA 0.0 \
    --repeatB 0.8 \
    --outputA dataset/tableA.csv \
    --outputB dataset/tableB.csv \
    --seedA 42 \
    --seedB 43
"""
import argparse
import random
import sys


def generate_pairs_with_repeats(num_rows, repeat_ratio, low=-1_000_000, high=1_000_000):
    """
    生成 key-value 结构，key 包含按比例重复值，用于 Spark Join。
    每行输出： key,value
    """
    if not (0.0 <= repeat_ratio <= 1.0):
        raise ValueError("repeat_ratio must be between 0.0 and 1.0")

    num_repeat = int(num_rows * repeat_ratio)
    num_unique = num_rows - num_repeat

    # 生成唯一 key
    unique_keys = [str(random.randint(low, high)) for _ in range(num_unique)]

    # 生成重复 key（热点 key）
    repeat_key = str(random.randint(low, high))
    repeat_keys = [repeat_key] * num_repeat

    # 合并 key
    all_keys = unique_keys + repeat_keys
    random.shuffle(all_keys)

    # 生成 value
    for key in all_keys:
        value = random.randint(1, 1000000)
        yield f"{key},{value}\n"


def write_file(filename, generator_fn):
    """统一的带缓冲写入方式"""
    with open(filename, "w", buffering=1024 * 1024) as f:
        for line in generator_fn:
            f.write(line)


def main():
    parser = argparse.ArgumentParser(description="生成 Spark Join 用的两个数据集（可复现、可控倾斜）")

    parser.add_argument("--num", type=int, default=10_000_000, help="每个数据集的记录数")
    parser.add_argument("--outputA", type=str, default="dataset/tableA.csv", help="输出文件 A")
    parser.add_argument("--outputB", type=str, default="dataset/tableB.csv", help="输出文件 B")

    parser.add_argument("--repeatA", type=float, default=0.0, help="A 的重复 key 比例")
    parser.add_argument("--repeatB", type=float, default=0.0, help="B 的重复 key 比例")

    parser.add_argument("--seedA", type=int, default=42, help="随机种子 A")
    parser.add_argument("--seedB", type=int, default=43, help="随机种子 B")

    args = parser.parse_args()

    print("=== Spark Join 数据生成 ===")
    print(f"A 总行数: {args.num:,}, 重复比例: {args.repeatA:.2%}, 种子: {args.seedA}")
    print(f"B 总行数: {args.num:,}, 重复比例: {args.repeatB:.2%}, 种子: {args.seedB}")
    print()

    # ---------- 生成 A ----------
    print(f"[A] 正在生成 {args.outputA} ...")
    random.seed(args.seedA)
    write_file(
        args.outputA,
        generate_pairs_with_repeats(args.num, args.repeatA),
    )
    print("[A] 完成\n")

    # ---------- 生成 B ----------
    print(f"[B] 正在生成 {args.outputB} ...")
    random.seed(args.seedB)
    write_file(
        args.outputB,
        generate_pairs_with_repeats(args.num, args.repeatB),
    )
    print("[B] 完成\n")

    print("生成结束！两个数据集已可用于 Spark Join 实验。")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n用户中断。")
        sys.exit(1)
    except Exception as e:
        print(f"错误: {e}")
        sys.exit(1)
