# Spark Streaming

from pyspark.streaming import StreamingContext

# Creating a streaming context with latency 3 seconds
streamContext = StreamingContext(SpContext,3)

#Reading lines from the socketTextStream setup through java program
tweetLines = streamContext.socketTextStream("localhost", 9000) #This is an RDD

#word count within tweetLines RDD
tweetWords = tweetLines.flatMap(lambda line: line.split(" "))
tweetPairs = tweetWords.map(lambda word: (word,1))
wordCounts = tweetPairs.reduceByKey(lambda x,y: x+y)
wordCounts.pprint(5)

#Count Lines using Global variables
totalLines = 0
linesCount = 0
def computeMetrics(rdd):
    global totalLines
    global linesCount
    linesCount = rdd.count()
    totalLines+=linesCount
    print(rdd.collect())
    print("Lines in RDD:",linesCount,"Total Lines:",totalLines)

tweetLines.foreachRDD(computeMetrics)

#Computing window metrics
def windowMetrics(rdd):
    print("Window RDD size:",rdd.count())

windowedRDD = tweetLines.window(6,3)
windowedRDD.foreachRDD(windowMetrics)

streamContext.start()
streamContext.stop()
print("Overall Lines:",totalLines)
