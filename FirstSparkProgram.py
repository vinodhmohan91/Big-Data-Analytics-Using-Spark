# First Spark Program
# File : movietweets.csv
# Objective : Display 5 records, convert to uppercase and count the records

#Create an RDD by loading from the file
tweetsRDD = SpContext.textFile("./Data/movietweets.csv")

#Read the first 5 records
tweetsRDD.take(5)

#Convert to uppercase and dispaly first 5 records
tweetsUpper = tweetsRDD.map(lambda x: x.upper()) #Transform Data
tweetsUpper.take(5)

#Count the number of tweets
tweetsRDD.count() #Action
