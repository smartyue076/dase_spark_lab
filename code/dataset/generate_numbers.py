import random
# from collections import Counter
# import matplotlib.pyplot as plt
import argparse
'''
usage:
python generate_numbers.py --n 50000000 --s 0 --output_file minimal_skew.txt
python generate_numbers.py --n 50000000 --s 0.5 --output_file moderate_skew.txt
python generate_numbers.py --n 50000000 --s 1 --output_file heavy_skew.txt
python generate_numbers.py --n 50000000 --s 2 --output_file full_skew.txt
'''


def generate_zipf_sequence(n, s, output_file, max_value=10000, plot=False):
    if s < 0:
        raise ValueError("s must be greater than or equal to 0")
    
    random.seed(42)

    # 生成 key 列表（从 1 到 max_value）
    keys = list(range(1, max_value + 1))

    # 根据 s 生成序列
    if s == 0:
        # 均匀分布
        seq = random.choices(keys, k=n)
    else:
        # Zipf 分布
        probs = [1 / (k ** s) for k in keys]
        total = sum(probs)
        probs = [p / total for p in probs]  # 归一化
        seq = random.choices(keys, weights=probs, k=n)

    # 写入文件
    # with open(output_file, "w") as f:
    #     for number in seq:
    #         f.write(f"{number}\n")
    # print(f"Generated sequence with n={n}, s={s}, saved to {output_file}")


    # use char pedding to format size
    width = len(str(max_value))

    with open(output_file, "w") as f:
        for number in seq:
            f.write(f"{number:0{width}d}\n")

    print(f"Generated sequence n={n}, s={s}, max_value={max_value}, saved to {output_file}")

    # # 绘制分布图
    # if plot:
    #     counter = Counter(seq)
    #     sorted_keys = sorted(counter.keys())
    #     counts = [counter[k] for k in sorted_keys]

    #     plt.figure(figsize=(12, 6))

    #     if max_value <= 1000:
    #         # 数据量小，直接用柱状图
    #         plt.bar(sorted_keys, counts, color='skyblue')
    #     else:
    #         # 数据量大，使用折线图或点图，减少点数显示整体趋势
    #         step = max(1, len(sorted_keys) // 1000)  # 保留约 1000 个点绘图
    #         plt.plot(sorted_keys[::step], counts[::step], color='skyblue', marker='o', markersize=2, linestyle='-')

    #     plt.xlabel("Key")
    #     plt.ylabel("Frequency")
    #     plt.title(f"{'Uniform' if s==0 else 'Zipf'} Distribution (s={s})")
    #     plt.grid(True)
    #     plt.show()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate a Zipf or Uniform-distributed sequence (pure Python)")
    parser.add_argument("--n", type=int, default=1000, help="Sequence size")
    parser.add_argument("--s", type=float, default=1.0, help="Skew parameter (s>=0), s=0 means uniform distribution")
    parser.add_argument("--output_file", type=str, default="sequence.txt", help="Output file path")
    parser.add_argument("--no_plot", action="store_true", help="Do not display distribution plot")

    args = parser.parse_args()

    generate_zipf_sequence(
        n=args.n,
        s=args.s,
        output_file=args.output_file,
        plot=not args.no_plot
    )
