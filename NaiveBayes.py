# Naive Bayes Classifier - Text Processing

# Loading the Data
smsData = SpContext.textFile("./Data/SMSSpamCollection.csv")
smsData.count()
smsData.take(5)
smsData.cache()

# Prepare data for Machine Learning
def TransformToVector(string):
    attList = string.split(",")
    smsType = 0.0 if attList[0] == "ham" else 1.0
    return [smsType,attList[1]]

smsTransformed = smsData.map(TransformToVector)

smsDF = SpSession.createDataFrame(smsTransformed,["label","message"])
smsDF.cache()
smsDF.select("label","message").show()

# Perform Machine Learning
# split into training and testing
(trainingData,testData) = smsDF.randomSplit([0.9,0.1])
trainingData.count()
testData.count()

#Setup pipeline
from pyspark.ml import Pipeline
from pyspark.ml.feature import HashingTF, IDF    
from pyspark.ml.feature import Tokenizer
from pyspark.ml.classification import NaiveBayes, NaiveBayesModel

tokenizer = Tokenizer(inputCol="message",outputCol="words")
hashingTF = HashingTF(inputCol = tokenizer.getOutputCol(),outputCol="tempfeatures")
idf = IDF(inputCol = hashingTF.getOutputCol(),outputCol="features")
nbClassifier = NaiveBayes()

pipeline = Pipeline(stages=[tokenizer,hashingTF,idf,nbClassifier])

#Build a model with Pipeline
nbModel = pipeline.fit(trainingData)

#Compute Predictions
prediction = nbModel.transform(testData)

#Evaluate Accuracy
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
evaluator = MulticlassClassificationEvaluator(predictionCol="prediction", \
                                              labelCol="label", \
                                              metricName = "accuracy")
evaluator.evaluate(prediction)

# Draw a confusion matrix
prediction.groupby("label","prediction").count().show()
