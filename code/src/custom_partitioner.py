import bisect
import math

"""
使用自适应分桶分区器时需要先进行抽样
samples = rdd.map(lambda x: int(x[0])).sample(False, 0.001).collect()
再实例化分区器
partitioner = AdaptiveBucketPartitioner(args.num_partitions, samples)
最后再进行分区
rdd = rdd.partitionBy(args.num_partitions, partitionFunc=partitioner)

"""




class AdaptiveBucketPartitioner:
    """
    自适应分桶分区器：
    - 基于 key 的真实分布（采样）
    - 热点区间自动分散到多个桶
    - 稀疏区间按范围分桶
    """
    def __init__(self, num_partitions, sample_keys):
        self.num_partitions = num_partitions
        self.boundaries = self._build_boundaries(sample_keys, num_partitions)

    def _build_boundaries(self, keys, num_parts):
        """
        根据采样 key 动态生成分桶边界
        目标：每个桶内 key 数量大致均衡
        """
        if len(keys) == 0:
            return [0] * num_parts
        
        keys = sorted(keys)
        n = len(keys)

        # 每个桶大约占多少采样量
        bucket_size = max(1, n // num_parts)

        boundaries = []
        for i in range(1, num_parts):
            idx = min(n - 1, i * bucket_size)
            boundaries.append(keys[idx])

        return boundaries  # 长度 num_parts-1

    def __call__(self, key):
        try:
            k = int(key)
        except:
            return 0
        
        # 二分查找对应桶
        return bisect.bisect_right(self.boundaries, k)

    def numPartitions(self):
        return self.num_partitions

