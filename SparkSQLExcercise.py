#Spark SQL Exercise

#Creating the dataframe from iris.csv
irisRDD = SpContext.textFile("./Data/iris.csv")
header = irisRDD.first()
irisRDDdata = irisRDD.filter(lambda x: x!=header)

irisParts = irisRDDdata.map(lambda x: x.split(","))
from pyspark.sql import Row
irisRows = irisParts.map(lambda x: Row(SepalLength = x[0],SepalWidth = x[1], \
                                       PetalLength = x[2],PetalWidth = x[3], \
                                       Species = x[4]))
irisDF = SpSession.createDataFrame(irisRows)
irisDF.show()

#Filter rows whose petal width is > 0.4
irisDF.filter(irisDF["PetalWidth"] > 0.4).count()

#Creating a Temp table iris and finding average petal width by species
irisDF.createOrReplaceTempView("iris")
SpSession.sql("Select Species, avg(PetalWidth) from iris group by Species").show()
