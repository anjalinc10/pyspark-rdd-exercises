from pyspark import SparkContext

sc = SparkContext("local[*]", "logLevelCount")
sc.setLogLevel("INFO")
base_rdd = sc.textFile("C:\\Users\\hp\\Desktop\\week10\\bigLog.txt")
mapped_rdd = base_rdd.map(lambda x: (x.split(":")[0], 1))
reduced_rdd = mapped_rdd.reduceByKey(lambda x,y: x+y)
result = reduced_rdd.collect()

for x in result:
    print(x)

