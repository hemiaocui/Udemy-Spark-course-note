# Udemy-Spark-course-note

# About

Here there are notes and tasks code for Udemy [Spark course](https://www.udemy.com/spark-and-python-for-big-data-with-pyspark/).

# Notes

## Set up

## Spark Dataframe Basics
```
from pyspark.sql import SparkSession
#build a session: 
spark=SparkSession.builder.appName('ops').getOrCreate()
data_schema=[StructField('age', IntegerType(),True)] #true means: allow null value

final_struc=StructType(fields=data_schema)
df=spark.read.json('people.json',schema=final_struc)

df.select['age'].show() #to see the content in the column; 
df.head(2) #row object
df.select(['age','name']).show()
df.withColumn('newage',df['age']).show() #the new column called newage is assigned to the same as age column)

df.createorReplaceTempView('people')
results=spark.sql("SELECT * FROM people")
results.show()

spark=SparkSession.builder.appName('ops').getOrCreate()
df=spark.read.csv('appl_stock.csv',inferSchema=True,header=True)
df.printSchema()
df.show()
df.head(3)

df.filter("Columnvalue<300").select(['Open','Close']).show() 
#use.collect() to collect result after filter; use row.asDict() to convert row object to dictionary; 

```
## Spark Dataframe Groupby and Aggregations
```
df.groupBy('Company').show()
df.groupBy('Company').count().show()
df.agg({'Sales':'sum'}).show()
#import functions: 
from pyspark.sql.functions import countDistinct,avg,stddev
df.select(avg('Sales')).show()
from pyspark.sql.functions import format_number
sales_std=df.select(stddev("Sales").alias('std')) #alias is for changing column name
sales_std.select(format_number('std',2).alias('std')).show()

df.orderBy("Sales").show()

```

## Spark Dataframe Missing Data
```
df.na.drop().show() #drop all rows that has any null
df.na.drop(thresh=2).show()
df.na.drop(how='all').show()
df.na.drop(subset=['Sales']).show()

df.na.fill(0).show()
df.na.fill('No Name',subset=['Name']).show()

mean_val=df.select(mean(df['Sales'])).collect()
mean_sales=mean_val[0][0]
df.na.fill(mean_sales,['Sales']).show()
```

```
```
