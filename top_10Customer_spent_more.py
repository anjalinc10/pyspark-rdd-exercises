from pyspark import SparkContext

sc = SparkContext("local[*]","top_10Customer")
rdd1 = sc.textFile("C:\\Users\\hp\\Desktop\\week9\\customerorders.csv")
rdd2 = rdd1.map(lambda x: (x.split(",")[0], float(x.split(",")[2])))
rdd3 = rdd2.reduceByKey(lambda x, y: x+y)
rdd4 = rdd3.sortBy(lambda x: x[1], False)

result = rdd4.collect()
for i in result:
    print(i)
