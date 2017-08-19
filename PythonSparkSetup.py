# Setting up Python and Spark
import os
import sys

# Working Directory
os.chdir("C:/Users/Vinodh Mohan/Documents/MSBAPM/Big Data Analytics using Spark")

# Spark Home
os.environ['SPARK_HOME'] = "C:/spark-2.0.0-bin-hadoop2.7"
SPARK_HOME = os.environ['SPARK_HOME'] #Creating a variable for usage

#Adding few system paths
sys.path.insert(0,os.path.join(SPARK_HOME,"python"))
sys.path.insert(0,os.path.join(SPARK_HOME,"python","lib"))
sys.path.insert(0,os.path.join(SPARK_HOME,"python","lib","pyspark.zip"))
sys.path.insert(0,os.path.join(SPARK_HOME,"python","lib","py4j-0.10.1-src.zip"))
sys.path.insert(0,os.path.join(SPARK_HOME,"jars"))

#Intialize Spark Session and SparkContext
from pyspark.sql import SparkSession
from pyspark import SparkContext

#Create a Spark Session
SpSession = SparkSession \
    .builder \
    .master("local") \
    .appName("First Session") \
    .config("spark.executor.memory","1g") \
    .config("spark.cores.max","2") \
    .config("spark.sql.warehouse.dir","file:///c:/temp/spark-warehouse") \
    .getOrCreate()
    
#Get the Spark Context from the Spark Session
SpContext = SpSession.sparkContext

#Test Spark
testData = SpContext.parallelize([3,6,4,2])
testData.count()

