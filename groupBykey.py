from pyspark import SparkContext

sc = SparkContext("local[*]", "logLevelCount")
sc.setLogLevel("INFO")
base_rdd = sc.textFile("C:\\Users\\hp\\Desktop\\week10\\bigLog.txt")
mapped_rdd = base_rdd.map(lambda x: (x.split(":")[0], x.split(":")[1]))
grouped_rdd = mapped_rdd.groupByKey()
final_rdd = grouped_rdd.map(lambda x: (x[0], len(x[1])))
result = final_rdd.collect()

for x in result:
    print(x)



