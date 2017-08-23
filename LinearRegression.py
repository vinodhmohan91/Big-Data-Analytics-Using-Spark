# Linear Regression

# Loading the Data
autoData = SpContext.textFile("./Data/auto-miles-per-gallon.csv")
autoData.count()
autoData.take(5)

# Remove the Header
autoData1 = autoData.filter(lambda line: "MPG" not in line)

# Cleaning the Data and creating a data frame
from  pyspark.sql import Row

# Setting a default value for HorsePower variable
avgHP = SpContext.broadcast(80.0)

def cleanUpData(inputString):
    global avgHP
    attList = inputString.split(",")
    
    if attList[3] == "?":
        attList[3] = avgHP.value
    
    # Create a row from cleaned data and convert to float
    rowLines = Row(MPG = float(attList[0]), \
                   CYLINDERS = float(attList[1]), \
                   DISPLACEMENT = float(attList[2]), \
                   HORSEPOWER = float(attList[3]),  \
                   WEIGHT = float(attList[4]),  \
                   ACCELERATION = float(attList[5]),  \
                   MODELYEAR = float(attList[6]),  \
                   NAME = attList[7])
    return rowLines

autoData2 = autoData1.map(cleanUpData)
autoData2.cache()
autoData2.take(5)

# Create a dataframe from the rows
autoDF = SpSession.createDataFrame(autoData2)
autoDF.show(5)

# Exploratory Analysis
autoDF.select("MPG","CYLINDERS").describe().show()

# Correlation Analysis
for i in autoDF.columns:
    if not(isinstance(autoDF.select(i).take(1)[0][0],str)):
        print("Correlation to MPG for ", i, autoDF.stat.corr('MPG',i))
        
# Preparing the Data for Machine Learning
from pyspark.ml.linalg import Vectors

# Creating a LabeledPoint and dropping columns with low correlation
def transformToLabeledPoint(autoRow):
    lp = (autoRow["MPG"],Vectors.dense([ autoRow["ACCELERATION"], \
                                       autoRow["DISPLACEMENT"], \
                                       autoRow["WEIGHT"] ]))
    return lp

autoLP = autoData2.map(transformToLabeledPoint)
autoDF1 = SpSession.createDataFrame(autoLP,["label","features"])
autoDF1.select("label","features").show(10)        

# Perform Machine Learning

#Split into training and test dataset
(trainingData, testData) = autoDF1.randomSplit([0.9,0.1])
trainingData.count()
testData.count()

#Building and fitting the Linear Regression model
from pyspark.ml.regression import LinearRegression
lr = LinearRegression(maxIter=10)
lrModel = lr.fit(trainingData)

# Print the coefficients and intercept
print("Coefficients: ",str(lrModel.coefficients))
print("Intercept: ",str(lrModel.intercept))

#predict on test data
predictions = lrModel.transform(testData)
predictions.select("prediction","label","features").show()

#Computing R2 value
from pyspark.ml.evaluation import RegressionEvaluator
evaluator = RegressionEvaluator(predictionCol="prediction",\
                                labelCol="label",\
                                metricName="r2")
evaluator.evaluate(predictions)
