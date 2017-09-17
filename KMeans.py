# K-Means Clustering

#Load the data
autoData = SpContext.textFile("./Data/auto-data.csv")
autoData.cache()

#Remove the header
firstline = autoData.first()
dataLines = autoData.filter(lambda x: x!=firstline)
dataLines.count()

from pyspark.sql import Row
import math
from pyspark.ml.linalg import Vectors

#Convert to Local Vector
def transformToNumeric(string):
    attList = string.split(",")
    
    doors = 1.0 if attList[3]=="two" else 2.0
    body = 1.0 if attList[4]=="sedan" else 2.0
    
    #Filter put columns not wanted and create Row
    Values = Row(DOORS=doors, \
                 BODY=body, \
                 HP = float(attList[7]), \
                 RPM = float(attList[8]), \
                 MPG = float(attList[9]))
    return Values

autoRows = dataLines.map(transformToNumeric)
autoRows.persist()
autoRows.collect()

autoDF = SpSession.createDataFrame(autoRows)

#Centering and Scaling
summaryStats = autoDF.describe().toPandas()
meanValues=summaryStats.iloc[1,1:5].values.tolist()
stdValues=summaryStats.iloc[2,1:5].values.tolist()

#place the mean and std values in broadcast variables
bcMeans = SpContext.broadcast(meanValues)
bcStdDev = SpContext.broadcast(stdValues)

def centerAndScale(inRow):
    global bcMeans
    global bcStdDev
    
    meanArray=bcMeans.value
    stdArray=bcStdDev.value
    
    retArray=[]
    for i in range(len(meanArray)):
        retArray.append((float(inRow[i])-float(meanArray[i]))/float(stdArray[i]))
    return Vectors.dense(retArray)

autoCS = autoDF.rdd.map(centerAndScale)
autoCS.collect()

#Creating a Spark DF again
autoFeatures = autoCS.map(lambda f: Row(features = f))
autoDF2 = SpSession.createDataFrame(autoFeatures)

from pyspark.ml.clustering import KMeans
Kmeans = KMeans(k=3,seed=1)
model = Kmeans.fit(autoDF2)
predictions = model.transform(autoDF2)
predictions.show()

#Plot the results in a scatter plot
import pandas as pd

def unstripData(string):
    return (string["prediction"], string["features"][0], \
            string["features"][1],string["features"][2], \
                  string["features"][3])

unstripped = predictions.rdd.map(unstripData)
predList = unstripped.collect()
predPd = pd.DataFrame(predList)

import matplotlib.pylab as plt
plt.cla()
plt.scatter(predPd[3],predPd[4],c = predPd[0])
plt.show()
