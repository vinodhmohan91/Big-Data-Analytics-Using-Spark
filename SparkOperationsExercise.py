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

