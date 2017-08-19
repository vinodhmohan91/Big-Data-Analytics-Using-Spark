#Spark SQL

# Creating a dataframe from JSON
empDF = SpSession.read.json("./Data/customerData.json")
empDF.show()
empDF.printSchema()

#Dataframe Queries
empDF.select("name").show()
empDF.filter(empDF["age"] == 40).show()
empDF.groupBy("gender").count().show()
empDF.groupBy("deptid").agg({"salary":"avg","age":"max"}).show()

# Creating a dataframe from a list
deptList = [{'name':'sales','id':'100'},{'name':'Engineering','id':'200'}]
deptDF = SpSession.createDataFrame(deptList)
deptDF.show()

#join the dataframes
empDF.join(deptDF, empDF.deptid == deptDF.id).show()

#Cascading Operations
empDF.filter(empDF["age"]>30).join(deptDF,empDF.deptid == deptDF.id). \
            groupBy("deptid"). \
            agg({"salary":"avg","age":"max"}).show()

# Creating a dataframe from RDD
autoRDD = SpContext.textFile("./Data/auto-data.csv")

#Remove header
autoRDDdata = autoRDD.filter(lambda x: "FUELTYPE" not in x)

#Splitting and converting RDD into rows
autoParts = autoRDDdata.map(lambda x: x.split(","))
from pyspark.sql import Row 
autoRows = autoParts.map(lambda x: Row(make = x[0],body = x[4], hp = int(x[7])))

#Creating dataframe from Rows
autoDF = SpSession.createDataFrame(autoRows)
autoDF.show()

#Creating a data frame from csv directly
autoDF1 = SpSession.read.csv("./Data/auto-data.csv",header=True)
autoDF1.show()


#Creating a Temp table / view
autoDF.createOrReplaceTempView("autoTable")
SpSession.sql("Select * from autoTable where hp > 200").show()

empDF.createOrReplaceTempView("empTable")
SpSession.sql("Select * from empTable where salary > 4000").show()


# Spark DF To Pandas DF
empPdDF = empDF.toPandas()
for index, row in empPdDF.iterrows():
    print (row["salary"])
    

# Connecting to SQL Database
DF = SpSession.read.format("jdbc").options(
        url = "jdbc:oracle:thin:@sb1-opimdb-1.business.uconn.edu:1521:d2",
        driver = "oracle.jdbc.OracleDriver",
        dbtable = "EMPLOYEES",
        user="vim15106",
        password="Dhoniv@19").load()

DF.show()
DF.select("JOB_ID").distinct().show()

