#!/usr/bin/env python
# coding: utf-8

# In[1]:


import findspark
findspark.init('/home/omotade/spark-3.3.1-bin-hadoop3/')


# In[2]:

print("Importing spark libraries and utilities>>>")
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as pysum, mean, min as pymin, max as pymax 


# In[4]:

print("Confguring Spark Session>>>")
spark = SparkSession.builder.\
        appName("TwitterCovidTweets").\
        config("spark.mongodb.input.uri", "mongodb://127.0.0.1/test.coll").\
        config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector:10.0.0').\
        config("spark.mongodb.output.uri", "mongodb://127.0.0.1/test.test").\
        getOrCreate()
print("Done")

# In[32]:


def case_count():
    return scrape()['cases']

spark.udf.register('get_case_count', case_count)


# In[6]:

print("Setting up connection to the stream and stream pipeline")
stream = spark.readStream.format('socket').\
                option('host', 'localhost').\
                option('port', '5555').\
                option('includeTimestamp', True).\
                load()


# In[7]:


print("Streaming Status:", stream.isStreaming)


# In[8]:


from scraping import scrape


# In[39]:


from pyspark.sql.functions import explode, split, col, lit
data = stream.select(split(stream.value, 'EOL').alias('content'), (stream.timestamp).alias('timestamp'))
data = data.withColumn('total_case_count', lit(case_count()))


# In[ ]:

print("Start Stream \n Stream data will be written directly to mongodb at localhost")
query = data.writeStream.\
                format("mongodb").\
                option("checkpointLocation", "/tmp/pyspark7/").\
                option("forceDeleteTempCheckpointLocation", "true")\
                .option('spark.mongodb.connection.uri', "mongodb://127.0.0.1/Covid")\
                .option('spark.mongodb.database', 'test')    .option('spark.mongodb.collection', 'CasesNTweets')\
                .trigger(processingTime = '20 seconds')\
                .outputMode("append")\
                .start()

query.awaitTermination()


# In[ ]:




