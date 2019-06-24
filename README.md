# Udemy-Spark-course-note

# About

Here there are notes and tasks code for Udemy [Spark course](https://www.udemy.com/spark-and-python-for-big-data-with-pyspark/).

## Spark Dataframe Basics
```
from pyspark.sql import SparkSession
#build a session: 
spark=SparkSession.builder.appName('ops').getOrCreate()
data_schema=[StructField('age', IntegerType(),True)] #true means: allow null value
data.describe().show()

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
df.descibe().printSchema()
df.show()
df.head(3)
df.describe().show()
df.filter("Columnvalue<300").count() #or import count function
df.filter("Columnvalue<300").select(['Open','Close']).show() 
#use.collect() to collect result after filter; use row.asDict() to convert row object to dictionary; 
df2=df.withColumn("HV Ratio",df['High']/df['Volume'])
from pyspark.sql.functions import corr
df.select(corr('High','Volume'))
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
df.orderBy(df['high'].desc()).show() #order by descending order 
df.orderBy(df['high'].desc()).head(1)[0][0]


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
## Spark Date and Timestamp
```
from pyspark.sql.functions import dayofmonth, hour, dayofyear, month, year,weekofyear
df.select(dayofmonth(df['Date']).show()
df.withColumn("Year",year[df['Date'])).show() #create new column called Year
newdf.groupby("Year").mean().select(['Year','avg(Close)'])
new.select(['Year',format_number('Average Closing Price',2).alias("Avg")]

monthdf.select(['Month','Close']).groupBy('Month').mean() #take the mean of close; 
```
## Spark with ML/linear regression:  
```
from pyspark.ml.regression import LinearRegression
training=spark.read.format('libsvm').load('sample_linear_regression_data.txt')
lr=LinearRegression(featuresCol='features',labelCol='label',predictCol='prediction')
lrMocel=lr.fit(training)
train_data,test_data=data_data.randomSplit([0.7,0.3])
test_resuls=model.evaluate(test_data)
predictions=model.transform(unlabeled_data)
```
## Spark with ML/logistic regression:  
```
model=LogisticRegression()
fitted=model.fit(mydata)
log_summary=fitted.summary()
log_summary.predictions.printSchema()

from pyspark.ml.evaluattion import (BinaryClassificationEvaluator, MulticlassClassficationEvaluator)
my_eval=BinaryClassificationEvaluator()
my_eval.evaluate(preciction_and_labels.predictions)

df=spark.sql("select * from titani_csv")
df.printSchema()
from pyspark.ml.feature import (VectorAssembler,VectorIndexer,OneHotEncoder,StringIndexer)
gender_indexer=StringIndexer(inputCol='Sex',outoutCol='SexIndex') #encode string to number; eg: A to 0, B to 1, C to 2;
gender_encoder=OneHotEncoder(inputCol='SexIndex',outputCol='SexVec')

assembler=VectorAssembler(inputCols=['Pclass','SexVec','EmbarkVec','Age','SibSp','Fare'],outputCol='feature')
from pyspark.ml import Pipeline
log_reg_titanic=LogisticRegression(featuresCol='features',labelCol='survived')
pipeline=Pipeline(stages=[gender_indexer,embark_indexer,gender_encoder,embarker_encoder,assembler, log_reg_titanic])
#stages mean do everything in the list one by one in the order; 
train_data,test_data=my_final_data.randomSplit([0.7,0.3])
fit_model=pipeline.fit(train_data)
results=fit_model.transform(test_data) #making prediction
my_eval=BinaryClassificationEvaluator(rawPredictionCol='prediction',labelCol='Survived')
AUC=my_eval.evaluate(results)
```

## Spark with ML/Tree methods:  
```
from pyspark.ml.classification import (RandomForrestClassifier, GBTclassifier,DecisionTreeClassifier)
acc_eval=MulticlassClassificationEvaluator(metricName='accuracy')

assembler=VectorAssembler(inputCols=['Apps','Accept','Enroll'],outputCol='features')
#https://spark.apache.org/docs/2.2.0/ml-features.html#vectorassembler
output=assembler.transform(data)
indexer=StringIndexer(inputCol='Private',outputCol='PrivateIndex')
output_fixed=indexer.fit(output).transform(output)
final_data=output_fixed.select('features','PrivateIndex')
train_data,test_data=final_data.randomSplit([0.7,0.3])
dtc=DecisionTreeClassifier(labelCol='PrivateIndex',featuresCol='features')
dtc_model=dtc.fit(train_data)
dtc_preds=dtc_model.transform(test_data)
```
## Spark with Kmeans methods:  
```
from pyspark.ml.feature import StandardScaler
scaler=StandardScaler(inputCol='features',outputCol='scaledFeatures')
scaler_model=scaler.fit(final_data)
cluster_final_data=scaler_model.transform(final_data)
kmeans2=KMeans(featuresCol='scaledFeatures',k=2)
model_k2=kmeans2.fit(cluster_final_data)
model_k2.transform(cluster_final_data)
```
## Spark with Recommendation system:  
```
from pyspark.ml recommendation import ALS # use alternating least square to learn latent factors; which uses matrix factorization to implement recommendation system 
from pyspark.ml.evaluation import RegressionEvaluator
als=ALS(maxIter=5,regParam=0.01,userCol='userId',itemCol='movieId', ratingCol='rating')
model=als.fit(training)
predictions=model.transform(test)
evaluator=RegressionEvaluator(metricName='rmse',labelCol='rating',predictionCol='prediction')
rmse=evaluator.evaluate(predictions)
#for predicting fresh user
single_user=test.filter(test['userId'==11).select(['movieId','userId'])
recommendations=model.transform(single_user)
recommendatios.orderBy('prediction',ascending=False).show()
```
## Spark with NLP: 
```
from pyspark.ml.feature import Tokenizer, RegexTokenizer
from pyspark.sql.functions impor col,udf
from pyspark.sql.types import IntegerType

sen_df=spark.createDataFrame([
(0,'Hi I head about'),
(1,'Logistirc,regression,models,are,neat')],
['id','sentence']) # id and sentence are the two column names; 
tokenizer=Tokenizer(inputCol='sentence',outputCol='words')
regex_tokenizer=RegexTokenizer(inputCol='sentence',outputCol='words',pattern='\\W') #
count_tokens=udf(lambda words:len(words),IntegerType)
tokenized=tokenizer.transform(sen_df)

from pyspark.ml.feature impor StopWordsRemover
remover=StopWordsRemover(inputCol='tokens',outputCol='filtered')

from pysparl.ml.feature import NGram
ngram=NGram(n=2,inputCol='words',outputCol='grams')
ngram.transform(wordDataFrame).select('grams').show(truncate=False)

from pyspark.ml.feature import HashingTF,IDF,Tokenizer
hashing_tf=HashingTF(inputCol='words',outputCol='rawFeatures')
#Maps a sequence of terms to their term frequencies using the hashing trick
featurized_data=hashing_tf.transform(words_data)
idf=IDF(inputCol='rawFeatures',outputCol='features')
idf_model=idf.fit(featurized_data)
rescaled_data=idf_model.transform(featurized_data)
#also, can use countvectorizer; 
```

## Spark Streaming
### Steps: create a sparkcontext, create Streaming context, socket text streaming, read in lines as 'D stream'
```
from pyspark import SparkContext
from pyspark. streaming import StreamingContext
sc=SparkContext('local[2]','NetworkWordCount') #2 working threads; 'NetworkWordCount' is the name for it; 
ssc=StreamingContext(sc,1) #take 1 sec at a time;
lines=ssc.socketTextStrema('localhost',9999) #9999 is the loalhost used; 
words=lines.flatMap(lambda line: line.split(' '))
pairs=words.map(lamdba word:(word,1))
word_counts=pairs.reduceByKey(lambda num1, numer2:num1+num2)
word_counts.pprint()
ssc.start()
Example of Tweet streaming
```
