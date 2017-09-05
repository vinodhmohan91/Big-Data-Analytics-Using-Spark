# Random Forest

# Loading the Data
bankData = SpContext.textFile("./Data/bank.csv")
bankData.count()
bankData.take(5)

# Remove the Header
firstLine = bankData.first()
bankData1 = bankData.filter(lambda line: line != firstLine)
bankData1.count()

# Reading the data using data frame
data = SpSession.read.csv("./Data/bank.csv",sep = ";",header="true")
data.show()
data.select("education").distinct().show()

# Cleaning the Data and creating a data frame
from pyspark.sql import Row
import math

def transformToNumeric(inputRDD):
    attList = inputRDD.replace("\"","").split(";") 
    
    #convert Age to float
    age = float(attList[0])
    
    #convert outcome to float
    outcome = 0.0 if attList[16] == "no" else 1.0
    
    #create indicator variables for marital column
    single = 1.0 if attList[2] == "single" else 0.0
    married = 1.0 if attList[2] == "married" else 0.0
    divorced = 1.0 if attList[2] == "divorced" else 0.0
                             
    #create indicator variables for education
    primary = 1.0 if attList[3] == "primary" else 0.0
    secondary = 1.0 if attList[3] == "secondary" else 0.0
    tertiary = 1.0 if attList[3] == "tertiary" else 0.0
                        
    #convert default to float
    default = 0.0 if attList[4] == "no" else 1.0
    
    #convert balance to float
    balance = float(attList[5])
    
    #convert loan to float
    loan = 0.0 if attList[7] == "no" else 1.0
    
    #create a row from cleaned and converted data
    bankRow = Row(OUTCOME = outcome, \
                  AGE = age, \
                  SINGLE = single, \
                  MARRIED = married, \
                  DIVORCED = divorced, \
                  PRIMARY = primary, \
                  SECONDARY = secondary, \
                  TERTIARY = tertiary, \
                  DEFAULT = default, \
                  BALANCE = balance, \
                  LOAN = loan
                  )
    
    return bankRow

bankRows = bankData1.map(transformToNumeric)
bankRows.collect()[:15]

bankData2 = SpSession.createDataFrame(bankRows)
bankData2.show()

# Exploratory Analysis
bankData2.describe("AGE","BALANCE","OUTCOME").show()

# Correlation Analysis
for i in bankData2.columns:
    if not(isinstance(bankData2.select(i).take(1)[0][0],str)):
        print("Correlation to OUTCOME for ", i, bankData2.stat.corr('OUTCOME',i))
        
        
# Preparing the Data for Machine Learning
from pyspark.ml.linalg import Vectors

# Creating a LabeledPoint
def transformToLabeledPoint(bankRows):
    lp = (bankRows["OUTCOME"],Vectors.dense([bankRows["AGE"], \
                                            bankRows["BALANCE"], \
                                            bankRows["DEFAULT"], \
                                            bankRows["LOAN"], \
                                            bankRows["SINGLE"], \
                                            bankRows["MARRIED"], \
                                            bankRows["DIVORCED"], \
                                            bankRows["PRIMARY"], \
                                            bankRows["SECONDARY"], \
                                            bankRows["TERTIARY"]
                                            ]))
    return lp

bankLP = bankRows.map(transformToLabeledPoint)
bankLP.collect()
bankDF = SpSession.createDataFrame(bankLP,["label","features"])
bankDF.select("label","features").show(10)

# Principal Component Analysis
from pyspark.ml.feature import PCA
bankPCA = PCA(k=3,inputCol="features",outputCol="pcaFeatures")
pcaModel = bankPCA.fit(bankDF)
pcaResult = pcaModel.transform(bankDF).select("label","pcaFeatures")
pcaResult.show(truncate=False)

#Split into training and test dataset
(trainingData, testData) = pcaResult.randomSplit([0.75,0.25])
trainingData.count()
testData.count()

#Building and fitting the Decision Tree model
from pyspark.ml.classification import RandomForestClassifier
classifier = RandomForestClassifier(labelCol="label",featuresCol="pcaFeatures")
clModel = classifier.fit(trainingData)

#Predict on test data
predictions = clModel.transform(testData)
predictions.select("prediction","label").show()

#Evaluate the accuracy
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
evaluator = MulticlassClassificationEvaluator(predictionCol="prediction", \
                                              labelCol="label", \
                                              metricName = "accuracy")
evaluator.evaluate(predictions)

# Draw a confusion matrix
predictions.groupby("label","prediction").count().show()
