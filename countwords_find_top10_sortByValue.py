from pyspark import SparkContext

sc = SparkContext("local[*]", "find_Top_10_SortByValue")
rdd1 = sc.textFile("C:\\Users\\hp\\Desktop\\week9\\search_data.txt")
rdd2 = rdd1.flatMap(lambda x: x.split(" "))
convert_lower = rdd2.map(lambda x: x.lower())
rdd3 = convert_lower.map(lambda x: (x, 1))
rdd4 = rdd3.reduceByKey(lambda x, y: x+y)
sort_result = rdd4.sortBy(lambda x: x[1], False)
result = sort_result.collect()

for i in result:
    words = i[0]
    count = i[1]
    print(f"{words} : {count}")
