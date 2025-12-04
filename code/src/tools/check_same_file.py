import hashlib

def file_md5(path):
    hash_md5 = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

def files_equal_md5(file1, file2):
    return file_md5(file1) == file_md5(file2)

print(files_equal_md5("/home/xuyue/shared_spark_test/xy/dase_spark_lab/code/dataset/heavy_skew.txt", 
                      "/home/xuyue/shared_spark_test/xy/dase_spark_lab/code/dataset/heavy_skew2.txt"))