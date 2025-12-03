from collections import Counter

def check_same_multiset(file1, file2):
    def load_counts(path):
        cnt = Counter()
        with open(path, "r") as f:
            for line in f:
                v = line.strip()
                if v:
                    cnt[v] += 1
        return cnt

    cnt1 = load_counts(file1)
    cnt2 = load_counts(file2)

    if cnt1 == cnt2:
        print("✔ 两个文件完全一致（排序正确）")
        return True
    else:
        print("✘ 文件内容不一致：")
        only_in_file1 = cnt1 - cnt2
        only_in_file2 = cnt2 - cnt1
        if only_in_file1:
            print("  - 原文件中多出的值：", list(only_in_file1.items())[:10])
        if only_in_file2:
            print("  - 排序后文件中多出的值：", list(only_in_file2.items())[:10])
        return False

check_same_multiset("/home/xuyue/shared_spark_test/xy/dase_spark_lab/code/dataset/test0.txt", 
                    "/home/xuyue/shared_spark_test/xy/dase_spark_lab/code/dataset/sorted_numbers-12030202/part-00000")