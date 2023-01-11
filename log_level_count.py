from pyspark import SparkContext

sc= SparkContext("local[*]", "logLevelCount")
sc.setLogLevel("INFO")

if __name__ == "__main__":
    log_list = ["WARN: Tuesday 4 September 0405",
                "WARN: Tuesday 4 September 0404",
                "INFO: Tuesday 4 September 0408",
                "WARN: Tuesday 4 September 0405",
                "INFO: Tuesday 4 September 0405",
                "WARN: Tuesday 4 September 0405",
                "INFO: Tuesday 4 September 0405"]
    original_logs_rdd = sc.parallelize(log_list)
else:
    original_logs_rdd = sc.textFile("C:\\Users\\hp\\Desktop\\week10\\bigLog.txt")
    print("inside the else part")

new_pair_rdd = original_logs_rdd.map(lambda x: (x.split(":")[0], 1))
resultant_rdd = new_pair_rdd.reduceByKey(lambda x,y: x+y)
result = resultant_rdd.collect()

for x in result:
    print(x)
    