def parseLine(line):
    fields = line.split("::")
    age = int(fields[2])
    num_friend = int(fields[3])
    return(age, num_friend)

from pyspark import SparkContext

sc = SparkContext("local[*]", "avg_of_customer")
inputFile = sc.textFile("C:\\Users\\hp\\Desktop\\week9\\friendsdata.csv")
rdd = inputFile.map(parseLine)
totalsByAge = rdd.mapValues(lambda x: (x, 1))\
    .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
averageByAge = totalsByAge.mapValues(lambda x: x[0]/x[1])
result = averageByAge.collect()

for i in result:
    print(i)