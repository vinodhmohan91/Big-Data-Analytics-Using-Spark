#Recommendation Engine

#Load the data
ratingsData = SpContext.textFile("./Data/UserItemData.txt")
ratingsData.collect()

#Convert the string into vector
ratingsVector = ratingsData.map(lambda l: l.split(',')) \
                           .map(lambda l: (int(l[0]), int(l[1]), float(l[2])))

ratingsDF = SpSession.createDataFrame(ratingsVector,['user','item','rating'])

#Building the model based on ALS
from pyspark.ml.recommendation import ALS
als = ALS(rank=10, maxIter=5)
model = als.fit(ratingsDF)

model.userFactors.orderBy("id").collect()

#Create a test data set of items you want ratings for
testDF = SpSession.createDataFrame([(1001,9003),(1001,9004),(1001,9005)], \
                                    ['user','item'])

#Predict
predictions = model.transform(testDF)
predictions.collect()
