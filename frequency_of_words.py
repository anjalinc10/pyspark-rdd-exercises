from pyspark import SparkContext

sc = SparkContext("local[*]", "word_count")
rdd1 = sc.textFile("C:\\Users\\hp\\Desktop\\week9\\search_data.txt")
rdd2 = rdd1.flatMap(lambda x: x.split(" "))
rdd3 = rdd2.map(lambda x: (x, 1))
rdd4 = rdd3.reduceByKey(lambda x, y: x+y)
result = rdd4.collect()

for i in result:
    print(i)

