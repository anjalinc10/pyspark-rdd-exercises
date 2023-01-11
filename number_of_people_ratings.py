from pyspark import SparkContext

sc = SparkContext("local[*]", "number_of_people_rating_count")
rdd1 = sc.textFile("C:\\Users\\hp\\Desktop\\week9\\moviedata.data")
rdd2 = rdd1.map(lambda x: x.split("\t")[2])
rdd3 = rdd2.map(lambda x: (x, 1))
rdd4 = rdd3.reduceByKey(lambda x, y: x+y)
result = rdd4.collect()

for i in result:
    print(f"movies rating: {i}")
