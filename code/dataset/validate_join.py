#!/usr/bin/env python3
"""
python3 validate_join.py \
  --tableA dataset/tableA.csv \
  --tableB dataset/tableB.csv \
  --join dataset/join_result-1764758849/part-00000
"""
import argparse

# --------------------------------------------------------------------
# 读取 CSV ("key,value")，返回 dict: key -> [v1, v2, ...]
# --------------------------------------------------------------------
def load_table(file_path):
    table = {}

    with open(file_path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            # 安全 CSV 解析
            pos = line.find(",")
            if pos == -1:
                continue  # 跳过坏行

            key = line[:pos]
            val = line[pos+1:]

            if key in table:
                table[key].append(val)
            else:
                table[key] = [val]

    return table


# --------------------------------------------------------------------
# 解析 join 输出 ('key', ('vA', 'vB'))
# 标准库实现：不用 regex，不用 eval，只写简单字符串解析器
# --------------------------------------------------------------------
def parse_join_line(line):
    """
    输入：('13453', ('198127', '701038'))
    输出：(key, valueA, valueB)
    """
    line = line.strip()

    # 格式必须以 "(" 开头
    if not (line.startswith("(") and line.endswith(")")):
        return None

    # 手工解析
    # 形式类似：
    # ('13453', ('198127', '701038'))
    # 去掉最外层括号
    content = line[1:-1].strip()

    # 第一个 key 在单引号里
    if not content.startswith("'"):
        return None

    # 找 key 的结束引号
    second_quote = content.find("'", 1)
    key = content[1:second_quote]

    # 找第一个逗号后开始第二部分
    rest = content[second_quote+1:].lstrip()
    if not rest.startswith(","):
        return None

    rest = rest[1:].lstrip()   # 去掉开头的 ","

    # rest 应该形如：('vA', 'vB')
    if not (rest.startswith("(") and rest.endswith(")")):
        return None

    kv = rest[1:-1].strip()

    # valueA
    if not kv.startswith("'"):
        return None

    end_va = kv.find("'", 1)
    vA = kv[1:end_va]

    remain = kv[end_va+1:].lstrip()

    if not remain.startswith(","):
        return None

    remain = remain[1:].lstrip()

    # valueB
    if not remain.startswith("'"):
        return None

    end_vb = remain.find("'", 1)
    vB = remain[1:end_vb]

    return key, vA, vB


# --------------------------------------------------------------------
# 加载 join 输出文件
# --------------------------------------------------------------------
def load_join_output(file_path):
    result = []

    with open(file_path) as f:
        for line in f:
            parsed = parse_join_line(line)
            if parsed is not None:
                result.append(parsed)
            else:
                print("⚠️ 无法解析行:", line.strip())

    return result


# --------------------------------------------------------------------
# Join 验证逻辑
# --------------------------------------------------------------------
def validate(join_results, tableA, tableB):

    ok = True

    print("🔍 校验每一条 join 记录...")

    for key, vA, vB in join_results:

        # 1. key 必须在 A 和 B 中
        if key not in tableA:
            print(f"❌ key={key} 不在 A 中")
            ok = False

        if key not in tableB:
            print(f"❌ key={key} 不在 B 中")
            ok = False

        # 2. vA 必须来自 tableA[key]
        if key in tableA and vA not in tableA[key]:
            print(f"❌ key={key} 的 vA={vA} 不在 A[key] 中")
            ok = False

        # 3. vB 必须来自 tableB[key]
        if key in tableB and vB not in tableB[key]:
            print(f"❌ key={key} 的 vB={vB} 不在 B[key] 中")
            ok = False

    print("\n🔍 校验笛卡尔积个数是否正确...")

    # 4. 检查 join 组合数量是否正确 (m×n)
    for key in tableA:
        if key in tableB:
            expected = len(tableA[key]) * len(tableB[key])
            actual = sum(1 for k, _, _ in join_results if k == key)

            if expected != actual:
                print(f"❌ key={key}: 期望 {expected} 条，但实际 {actual} 条")
                ok = False

    if ok:
        print("\n🎉 Join 校验全部通过！")
    else:
        print("\n❌ Join 校验失败，请查看上面的错误信息。")


# --------------------------------------------------------------------
# 主程序入口
# --------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tableA", required=True)
    parser.add_argument("--tableB", required=True)
    parser.add_argument("--join", required=True)
    args = parser.parse_args()

    print("📥 加载表 A...")
    tableA = load_table(args.tableA)

    print("📥 加载表 B...")
    tableB = load_table(args.tableB)

    print("📥 加载 Join 输出...")    
    join_results = load_join_output(args.join)

    print("🚀 开始验证 Join 正确性...\n")
    validate(join_results, tableA, tableB)


if __name__ == "__main__":
    main()
