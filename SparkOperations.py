# Loading and Storing data

#<----------Loading---------->
#From a collection
collData = SpContext.parallelize([3,4,5,6,5])
collData.collect()

#From a CSV file
autoData = SpContext.textFile("./Data/auto-data.csv")
autoData.cache()

autoData.take(5) #First 5 rows
autoData.first() #first row
autoData.count() #Count

#Using for loop to collect each line and print the same
for line in autoData.collect():
    print(line)              

#Storing the data into a csv file
autoDataFile = open("./Data/auto-data-saved.csv","w")
autoDataFile.write("\n".join(autoData.collect()))
autoDataFile.close()


#<----------Transformations---------->

#Map - create a new RDD
tabData = autoData.map(lambda x: x.replace(",","/t"))
tabData.take(5)

#Filter
toyotaData = autoData.filter(lambda x: "toyota" in x)
toyotaData.count()

#FlatMap
words = toyotaData.flatMap(lambda line: line.split(","))
words.count()
words.take(5)

#Distinct
for distData in collData.distinct().collect():
    print(distData)
    
#Set - Union and Intersection
words1 = SpContext.parallelize(["Car","Bike","Van"])
words2 = SpContext.parallelize(["Car","Jeep","Tempo"])

for unions in words1.union(words2).distinct().collect():
    print(unions)
for intersections in words1.intersection(words2).collect():
    print(intersections)
    
#Cleanse and transform an RDD
def CleanseRDD(autoStr):
    if isinstance(autoStr,int):
        return autoStr
    
    autoAtt = autoStr.split(",")
    #convert doors to number
    if autoAtt[3] == "two":
        autoAtt[3] = "2"
    else:
        autoAtt[3] = "4"
    #convert drive to uppercase
    autoAtt[5] = autoAtt[5].upper()
    
    return ",".join(autoAtt)

cleanedData = autoData.map(CleanseRDD)
cleanedData.collect()

#<--------------Actions----------------->

        
        
        
