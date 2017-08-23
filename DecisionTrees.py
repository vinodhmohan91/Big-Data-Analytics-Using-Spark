# Decision Trees

# Loading the Data
irisData = SpContext.textFile("./Data/iris.csv")
irisData.count()
irisData.take(5)

# Remove the Header
irisData1 = autoData.filter(lambda line: "Species" not in line)

# Cleaning the Data and creating a data frame
from  pyspark.sql import Row
    
# Create a row from cleaned data and convert to float
irisParts = irisData1.map(lambda line: line.split(","))
irisRows =  irisParts.map(lambda parts: Row(SepalLength = float(parts[0]), \
                   SepalWidth = float(parts[1]), \
                   PetalLength = float(parts[2]), \
                   PetalWidth = float(parts[3]),  \
                   Species = parts[4]))

# Create a dataframe from the rows
irisDF = SpSession.createDataFrame(irisRows)
irisDF.show(5)
irisDF.cache()

#Adding a numeric indexer for the label/target column
from pyspark.ml.feature import StringIndexer
stringIndexer = StringIndexer(inputCol="Species",outputCol="Index_Species")
si_model = stringIndexer.fit(irisDF)
irisDF1 = si_model.transform(irisDF)

irisDF1.select("Species","Index_Species").distinct().show()

#Exploratory Analysis
irisDF1.describe().show()

# Correlation Analysis
for i in irisDF1.columns:
    if not(isinstance(irisDF1.select(i).take(1)[0][0],str)):
        print("Correlation to MPG for ", i, irisDF1.stat.corr('Index_Species',i))
        
# Preparing the data for machine learning
from pyspark.ml.linalg import Vectors
def transformToLabeledPoint(irisRow):
    lp = (irisRow["Species"],irisRow["Index_Species"], \
                             Vectors.dense([irisRow["PetalLength"], \
                                            irisRow["PetalWidth"], \
                                            irisRow["SepalLength"], \
                                            irisRow["SepalWidth"] ]))
    return lp

irisLP = irisDF1.rdd.map(transformToLabeledPoint)
irisDF2 = SpSession.createDataFrame(irisLP,["Species","label","features"])
irisDF2.show(5)

# Perform Machine Learning

#Split into training and test dataset
(trainingData, testData) = irisDF2.randomSplit([0.9,0.1])
trainingData.count()
testData.count()

#Building and fitting the Decision Tree model
from pyspark.ml.classification import DecisionTreeClassifier
classifier = DecisionTreeClassifier(maxDepth=2, labelCol="label",featuresCol="features")
clModel = classifier.fit(trainingData)

clModel.numNodes
clModel.depth

#Predict on test data
predictions = clModel.transform(testData)
predictions.select("prediction","species","label").show()

#Evaluate the accuracy
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
evaluator = MulticlassClassificationEvaluator(predictionCol="prediction", \
                                              labelCol="label", \
                                              metricName = "accuracy")
evaluator.evaluate(predictions)

# Draw a confusion matrix
predictions.groupby("label","prediction").count().show()






