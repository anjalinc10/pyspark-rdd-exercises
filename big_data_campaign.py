from pyspark import SparkContext

sc = SparkContext("local[*]", "bigDataCampaign")
input_file = sc.textFile("C:\\Users\\hp\\Desktop\\week10\\bigdatacampaigndata.csv")
mapped_input = input_file.map(lambda x: (float(x.split(",")[10]), x.split(",")[0]))
words = mapped_input.flatMapValues(lambda x: x.split(" "))
final_mapped = words.map(lambda x: (x[1].lower(), x[0]))
total = final_mapped.reduceByKey(lambda x,y: x+y)
sorted = total.sortBy(lambda x: x[1], False)
result = sorted.take(20)

for x in result:
    print(x)
