from pyspark import SparkContext
def blankLineChecker(line):
    if(len(line) == 0):
        myaccum.add(1)

sc = SparkContext("local[*]", "AccumulatorExample")
myrdd = sc.textFile("C:\\Users\\hp\\Desktop\\week10\\Accumulator.txt")
myaccum = sc.accumulator(0.0)
myrdd.foreach(blankLineChecker)
print(f"number of blank lines: {myaccum.value}")

