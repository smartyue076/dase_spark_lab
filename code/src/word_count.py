from pyspark.sql import SparkSession

def word_count_example():
    # 创建SparkSession
    spark = SparkSession.builder \
        .appName("WordCountExample") \
        .getOrCreate()
    
    # 获取SparkContext
    sc = spark.sparkContext
    sc.setLogLevel("WARN")
    
    print("=== Spark单词计数示例 ===")
    
    # 示例文本数据
    text_data = [
        "Hello Spark Hello World",
        "Spark is fast and powerful",
        "Hello again Spark world", 
        "Big data processing with Spark",
        "Learning Spark is fun and exciting"
    ]
    
    # 创建RDD
    rdd = sc.parallelize(text_data, 4)
    print(f"原始数据分区数: {rdd.getNumPartitions()}")
    
    # 显示原始数据
    print("\n=== 原始文本数据 ===")
    for i, line in enumerate(rdd.collect()):
        print(f"行 {i+1}: {line}")
    
    # 单词计数处理流程
    print("\n=== 开始单词计数处理 ===")
    
    # 1. 切分单词
    words_rdd = rdd.flatMap(lambda line: line.split(" "))
    print(f"切分后单词数: {words_rdd.count()}")
    
    # 2. 转换为小写并过滤空字符串
    clean_words_rdd = words_rdd.map(lambda word: word.strip().lower()) \
                              .filter(lambda word: len(word) > 0)
    
    # 3. 转换为键值对 (word, 1)
    word_pairs_rdd = clean_words_rdd.map(lambda word: (word, 1))
    
    # 4. 按单词聚合计数
    word_counts_rdd = word_pairs_rdd.reduceByKey(lambda a, b: a + b)
    
    # 5. 按计数降序排序
    sorted_word_counts = word_counts_rdd.sortBy(lambda x: x[1], ascending=False)
    
    # 收集最终结果
    print("\n=== 单词计数结果 (按频率降序) ===")
    results = sorted_word_counts.collect()
    
    # 打印结果表格
    print(f"{'单词':<15} {'出现次数':<10}")
    print("-" * 25)
    for word, count in results:
        print(f"{word:<15} {count:<10}")
    
    # 统计信息
    total_words = sum(count for _, count in results)
    unique_words = len(results)
    
    print(f"\n=== 统计摘要 ===")
    print(f"总单词数: {total_words}")
    print(f"唯一单词数: {unique_words}")
    print(f"最频繁单词: '{results[0][0]}' (出现{results[0][1]}次)")
    
    # 停止SparkSession
    spark.stop()
    print("\n=== 执行完成 ===")

# 运行示例
if __name__ == "__main__":
    word_count_example()