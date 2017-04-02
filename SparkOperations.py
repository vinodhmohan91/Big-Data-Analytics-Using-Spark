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

#Reduce - compute the sum
collData.reduce(lambda x,y: x+y)        

#Finding the longest line
autoData.reduce(lambda x,y: x if len(x)>len(y) else y) 

#Find the average MPFG-City for all cars
def getMPG(autoStr):
    if isinstance(autoStr,int):
        return autoStr
    autoAtt = autoStr.split(",")
    if autoAtt[9].isdigit():
        return int(autoAtt[9])
    else:
        return 0

#Average MPG-City
autoData.reduce(lambda x,y: getMPG(x)+getMPG(y)) / (autoData.count()-1)

#<--------------Key-Value RDDs-------------->
#Defining a Key-Value RDD
HPData = autoData.map(lambda x: (x.split(",")[0],x.split(",")[7]))
HPData.collect()

#Getting the keys alone
HPData.keys().distinct().collect()

#Removing the Header
header = HPData.first()
HPData1 = HPData.filter(lambda x: x!=header)

#Adding one to calculate the average for each key (MAKE)
HPData2 = HPData1.mapValues(lambda x: (x,1))
HPData2.collect()

#Finding the aggregate(sum) of values
HPData3 = HPData2.reduceByKey(lambda x,y: (int(x[0])+int(y[0]), x[1]+y[1]))
HPData3.collect()

#Finding the average HP summarized by Make
HPDataSummary = HPData3.mapValues(lambda x: int(x[0])/int(x[1]))
HPDataSummary.collect()

#<-----Broadcasting, Accumulation, Persistence and Patritioning----->

#Defining Accumulator variables
sedanCount = SpContext.accumulator(0)
hatchCount = SpContext.accumulator(0)

#Defining broadcast variables
sedanText = SpContext.broadcast("sedan")
hatchText = SpContext.broadcast("hatchback")

#Function to count the sedan, hatchback using broadcast variables and also split lines
def splitlines(line):
    global sedanCount
    global hatchCount
    
    if sedanText.value in line:
        sedanCount += 1
    if hatchText.value in line:
        hatchCount += 1
        
    return line.split(",")

#Map
splitData = autoData.map(splitlines)

#Lazy execution
splitData.count()
print(sedanCount,hatchCount)

#Partition
collData.getNumPartitions()

collData = SpContext.parallelize([3,4,5,6,5],4) #Partition into 4
collData.cache() #Persistence - caching
collData.count()

collData.getNumPartitions()    
    
         
