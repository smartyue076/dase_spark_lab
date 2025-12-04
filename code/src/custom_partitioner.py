import bisect
import math
from collections import Counter
class HotKeyAwarePartitioner:
    def __init__(self, numPartitions, hot_keys, hot_replica=8):
        self.numPartitions = numPartitions
        self.hot_keys = set(hot_keys)
        self.hot_replica = hot_replica

    def __call__(self, key):
        try:
            k = int(key)
        except:
            return 0

        if k in self.hot_keys:
            # 对热点 key 随机或 hash 拆分
            return hash((k, random.randint(0, self.hot_replica))) % self.numPartitions

        else:
            # 普通 key 正常哈希
            return hash(k) % self.numPartitions

    def numPartitions(self):
        return self.numPartitions


"""
使用自适应分桶分区器时需要先进行抽样以及计算热点数据
samples = rdd.map(lambda x: int(x[0])).sample(False, 0.001).collect()
cnt = Counter(samples)
# 选出现前 50 个 key（根据数据倾斜程度可调）
hot_keys = [k for k,_ in cnt.most_common(50)]
再实例化分区器
partitioner = HotKeyAwarePartitioner(
    args.num_partitions,
    hot_keys
)
最后再进行分区
rdd = rdd.partitionBy(args.num_partitions, partitionFunc=partitioner)

"""

