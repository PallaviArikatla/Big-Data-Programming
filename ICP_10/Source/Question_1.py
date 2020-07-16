import csv
from pyspark.sql import SparkSession
import pyspark.sql.functions as f

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .getOrCreate()

#part 1 question 1
df = spark.read.csv(r"C:\Users\pallavi\PycharmProjects\BDP_ICP10\survey.csv",header=True)
df.createOrReplaceTempView("Survey")

#part 1 question 2
df.write.option("header", "true").csv("spark_survey3.csv")

#part 1 question 3
print(df.dropDuplicates().count())
df.groupBy(df.columns)\
.count()\
.where(f.col('count') > 1)\
.select(f.sum('count'))\
.show()

#part 1 question 4
spark.sql("select * from Survey where Gender = 'Male' or Gender = 'M' or Gender='male'").createTempView("Table_Male")
spark.sql("select * from Survey where Gender = 'Female' or Gender = 'female'").createTempView("Table_Female")
spark.sql("select * from Table_Male union select * from Table_Female order by Country ").show(50)

#part 1 question 5
spark.sql("select treatment,count(*) as count from Survey group by treatment").show()


# part 2 question 1
spark.sql("select m.age,m.Country,m.Gender, m.treatment,f.Gender,f.treatment from Table_Male m join Table_Female f on m.Country = f.Country").show()
spark.sql("select sum(Age),count(Gender) from Survey").show()
spark.sql("select m.age,m.Country,m.Gender, m.treatment,f.Gender,f.treatment from Table_Male m left join Table_Female f on m.State = f.State").show()
#part 2 question 2
spark.sql("select * from Survey ORDER BY Timestamp limit 13").createTempView("Test")
spark.sql("select * from Test order by Timestamp desc limit 1").show()