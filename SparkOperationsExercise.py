# Spark Operations Practice

#Loading the iris.csv file as RDD
irisRDD = SpContext.textFile("./Data/iris.csv")
irisRDD.cache()

irisRDD.count() #Checking the count
irisRDD.take(5)

#Filtering out the header
header = irisRDD.first()
irisData = irisRDD.filter(lambda x: x!=header)

#Transforming the numberic values into float datatype
def numericToFloat(irisRecord):
    irisCells = irisRecord.split(",")
    for i in range(len(irisCells)-1):
        irisCells[i] = str(float(irisCells[i]))
    return ",".join(irisCells)

irisDataTr = irisData.map(numericToFloat) #Transformed irisData
irisDataTr.collect()

#Filtering the irisRDD to get flowers of species 'versicolor'
irisVersicolor = irisRDD.filter(lambda flower: "versicolor" in flower)
irisVersicolor.collect()

#Action - Find the average Sepal.Length for all flowers in irisRDD
#User definted function to check whether a string contains a float or not
def isfloat(value):
  try:
    float(value)
    return True
  except ValueError:
    return False

#User defined function to get the Sepal.length inside an RDD
def getSepLen(irisRecord):
    if isinstance(irisRecord,float):
        return irisRecord
    irisCells = irisRecord.split(",")
    if isfloat(irisCells[0]):
        return float(irisCells[0])
    else:
        return 0.0

#Average Sepal.Length
avgSepLength = irisDataTr.reduce(lambda x,y: getSepLen(x) + getSepLen(y)) / irisDataTr.count()

#Key-Value RDDs - Species as key and Sepal.Length as value
SepalData = irisDataTr.map(lambda x: (x.split(",")[4], x.split(",")[0]))
SepalData.collect()

#Find the minimum of Sepal.Length by each species
SepalMin = SepalData.reduceByKey(lambda x,y: x if float(x)<float(y) else y)
SepalMin.collect()

#Finding the no.of.records in Sepal.Length that is greater than avg Sepal Length we found
sepalCount = SpContext.accumulator(0)
sepalAvg = SpContext.broadcast(avgSepLength)

def NumOfSepals(value):
    global sepalCount
    if float(value[1]) > sepalAvg.value:
        sepalCount += 1
    return value
    
sepalData1 = SepalData.map(NumOfSepals)
sepalData1.count()
print(sepalCount)
