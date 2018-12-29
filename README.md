# Chapter 1. Introduction to Data Analysis with Spark (15)



- Spark SQL
- Spark Streaming
- MLlib
- GraphX


### For running `pyspark`
```
export SPARK_HOME=/usr/local/Cellar/apache-spark/2.4.0/libexec/

export PYTHONPATH=/usr/local/Cellar/apache-spark/2.4.0/libexec/python/:$PYTHONP$

pyspark

```



### For running python3:
```
export PYSPARK_PYTHON=python3    # Fully-Qualify this if necessary. (python3)
export PYSPARK_DRIVER_PYTHON=ptpython3  # Fully-Qualify this if necessary. (ptpython3)

```



**resilient distributed dataset (RDD)**

[Chapter 3](#anchors-in-markdown)

# Chapter 2. Downloading Spark and Getting Started (31)

### Standalone Applications

`bin/spark-submit my_script.py`


install pyspark:
```
# install Homebrew
/usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
#
brew install apache-spark
#
brew cask install caskroom/versions/java8

```

(#anchors-in-markdown)

### example
```
from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local").setAppName("My App")
sc = SparkContext(conf = conf)

rdd = sc.textFile("aa.txt")
res1 = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))

```



### example
```
from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local").setAppName("My App")
sc = SparkContext(conf = conf)
```
`local` is a special value that runs Spark on one thread on the local machine, without connecting to a cluster.


# Chapter 3. Programming with RDDs (46)

Users create RDDs in two ways:
- by loading an external dataset, or
- by distributing acollection of objects (e.g., a list or set) in their driver program.

Once created, RDDs offer two types of operations: **transformations** and **actions**.Transformations construct a new RDD from a previous one.
one common transformation is filtering data


```
pythonLines = lines.filter(lambda line: "Python" in line)
```

Actions, on the other hand, compute a result based on an RDD, and either return it to thedriver program or save it to an external storage system (e.g., HDFS). One example of anaction we called earlier is `first()`, which returns the first element in an RDD
`pythonLines.first()`

for the `first()` action, Spark scans the file only until it finds the first matching line; it doesn’t even read the whole file.

Spark’s RDDs are by default recomputed each time you run an action on them.

If you would like to reuse an RDD in multiple actions, you can ask Spark to persist it using `RDD.persist()`.


We can ask Spark to persist our data in a number of different places,which will be covered in Table 3-6. After computing it the first time, Spark will store the RDD contents in memory (partitioned across the machines in your cluster), and reusethem in future actions. Persisting RDDs on disk instead of memory is also possible.


### Example: Persisting an RDD in memory

```
pythonLines.persist
pythonLines.count()
pythonLines.first()
```

To summarize, every Spark program and shell session will work as follows:
   1. Create some input RDDs from external data.
   2. Transform them to define new RDDs using transformations like `filter()`.
   3. Ask Spark to `persist()` any intermediate RDDs that will need to be reused.
   4. Launch actions such as `count()` and `first()` to kick off a parallel computation,which is then optimized and executed by Spark.



`cache()` is the same as calling persist() with the default storage level.


The simplest way to create RDDs is to take an existing collection in your program andpass it to SparkContext’s `parallelize()` method,

```
lines = sc.parallelize(["pandas", "i like pandas"])
```

A more common way to create RDDs is to load data from external storage.
```
lines = sc.textFile("/path/to/README.md")
```

transformed RDDs are computed lazily, only when you use them in anaction. Many transformations are element-wise; that is, they work on one element at atime;

```
inputRDD = sc.textFile("log.txt")
errorsRDD = inputRDD.filter(lambda x: "error" in x)
```


Note that the `filter()` operation does not mutate the existing inputRDD. Instead, it returns a pointer to an entirely new RDD. inputRDD can still be reused later in the program


```
errorsRDD = inputRDD.filter(lambda x: "error" in x)
warningsRDD = inputRDD.filter(lambda x: "warning" in x)badLinesRDD = errorsRDD.union(warningsRDD)
```


`take()`, which collects a number of elements from the RDD


Python error count using actionsprint:
```
"Input had " + badLinesRDD.count() + " concerning lines"
print "Here are 10 examples:"
for line in badLinesRDD.take(10):
    print line
```


transformations on RDDs are lazily evaluated, meaning that Sparkwill not begin to execute until it sees an action.

### lazily evaluated
Loading data into an RDD is lazily evaluated.


## Passing functions
-  In Python, we have three options for passing functions into Spark. For shorter functions,we can pass in `lambda` expressions
```
word = rdd.filter(lambda s: "error" in s)
```
- Alternatively, we can pass in top-level functions, or locally definedfunctions.
```
def containsError(s):	return "error" in sword = rdd.filter(containsError)
```
The `filter()` transformation takes in a function and returns an RDD
that only has elements that pass the `filter()` function.
- The `map()` transformation takes in a function and applies it to eachelement in the RDD with the result of the function being the new value of each element inthe resulting RDD.


## parallelize
```
nums = sc.parallelize([1, 2, 3, 4])
squared = nums.map(lambda x: x * x).collect()
for num in squared:
    print "%i " % (num)
```

Sometimes we want to produce multiple output elements for each input element. Theoperation to do this is called `flatMap()`.
```
lines = sc.parallelize(["hello world", "hi"])
words = lines.flatMap(lambda line: line.split(" "))
words.first() # returns "hello"
```


## More functions
```
rdd1.distinct()
rdd1.union(rdd2)
rdd1.intersection(rdd2)
rdd1.subtract(rdd2)
rdd1.cartesian(rdd2)
```


### reduce
`reduce()`, which takes a function that operates on two elements of the type in your RDD and returns a new element of the same type.
```
sum = rdd.reduce(lambda x, y: x + y)
```

### fold
Similar to `reduce()` is `fold()`, which also takes a function with the same signature asneeded for `reduce()`, but in addition takes a "zero value" to be used for the initial call oneach partition.


### aggregate
The `aggregate()` function frees us from the constraint of having the return be the sametype as the RDD we are working on. With `aggregate()`, like `fold()`, we supply an initialzero value of the type we want to return. We then supply a function to combine theelements from our RDD with the accumulator. Finally, we need to supply a secondfunction to merge two accumulators, given that each node accumulates its own resultslocally.

```
sumCount = nums.aggregate((0, 0),	(lambda acc, value: (acc[0] + value, acc[1] + 1),	(lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1]))))	return sumCount[0] / float(sumCount[1])
```


```
t1 = sc.parallelize([1,2,3,4,5,6,7])
t1.aggregate(0,lambda x,y: x+y, lambda a,b: a+b)
# 28
```



### collect
`collect()`, which returns the entire RDD’s contents


### take
`take(n)` returns n elements from the RDD and attempts to minimize the number ofpartitions it accesses, so it may represent a biased collection

### top
`top()`


### takeSample
`takeSample(withReplacement, num, seed)` function allows us to take a sample of ourdata either with or without replacement.


### countByValue
`countByValue()`

### takeOrdered
`takeOrdered(num)(ordering)`

```
reduce(func) # rdd.reduce((x, y) => x + y)
rdd.fold(0)((x, y) => x + y)
```

### foreach
`foreach(func)`, Apply the provided function to each element of the RDD.


## Persistence (Caching)
```
MEMORY_ONLY
MEMORY_ONLY_SER
MEMORY_AND_DISK
MEMORY_AND_DISK_SER
```

```
val result = input.map(x => x * x)result.persist(StorageLevel.DISK_ONLY)println(result.count())println(result.collect().mkString(","))
```


### unpersist
`unpersist()` that lets you manually remove them from the cache.







# Chapter 4. Working with Key/Value Pairs (75)
Key/value RDDs are commonly used to
perform aggregations, and often we will do some initial ETL (extract, transform, and load)
to get our data into a key/value format. Key/value RDDs expose new operations (e.g.,
counting up reviews for each product, grouping together data with the same key, and
grouping together two different RDDs).


We also discuss an advanced feature that lets users control the layout of pair RDDs across
nodes: partitioning (reduce communication costs).

### Example
Creating a pair RDD using the first word as the key in Python
```
pairs = lines.map(lambda x: (x.split(" ")[0], x))
```


When creating a pair RDD from an in-memory collection in Scala and Python, we only
need to call `SparkContext.parallelize()` on a collection of pairs.


## Transformations on Pair RDDs
Pair RDDs are allowed to use all the transformations available to standard RDDs. The
same rules apply from "Passing Functions to Spark".

Since pair RDDs contain tuples, we
need to pass functions that operate on tuples rather than on individual elements.

## some functions


- `reduceByKey(func)`
- `aggregateByKey`
- `groupByKey()`
- `combineByKey(createCombiner, mergeValue, mergeCombiners, partitioner)`
- `mapValues(func)`
- `flatMapValues(func)`
- `keys()`
- `values()`
- `sortByKey()`
- `join`
- `rightOuterJoin`
- `leftOuterJoin`
- `cogroup`


```
t = sc.parallelize([(1, 2), (3, 4), (4, 6), (1,8)])
t.reduceByKey(lambda x,y: x+y).collect()
# [(1, 10), (3, 4), (4, 6)]

####
t.aggregateByKey(0,lambda x,y: x+y,lambda x,y: x+y).collect()
# [(1, 10), (3, 4), (4, 6)]

#####
t.groupByKey().collect()

#####
t2 = sc.parallelize([(1, 200), (3, 400), (1,800)])

t.join(t2)
# [(1, (2, 200)), (1, (2, 800)), (1, (8, 200)), (1, (8, 800)), (3, (4, 400))]

```




### Example
Simple filter on second element in Python
```
result = pairs.filter(lambda keyValue: len(keyValue[1]) < 20)
```


## Aggregations
### Example
Per-key average with `reduceByKey()` and `mapValues()` in Python
```
rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
```



### Example
Word count in Python
```
rdd = sc.textFile("s3://…")
words = rdd.flatMap(lambda x: x.split(" "))
result = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
```

### Example
Per-key average using `combineByKey()` in Python
```
sumCount = nums.combineByKey((lambda x: (x,1)),
(lambda x, y: (x[0] + y, x[1] + 1)),
(lambda x, y: (x[0] + y[0], x[1] + y[1])))
sumCount.map(lambda key, xy: (key, xy[0]/xy[1])).collectAsMap()
```



### Example
`reduceByKey()` with custom parallelism in Python
```
data = [("a", 3), ("b", 4), ("a", 1)]
sc.parallelize(data).reduceByKey(lambda x, y: x + y) # Default parallelism
sc.parallelize(data).reduceByKey(lambda x, y: x + y, 10) # Custom parallelism
```


### Example
Custom sort order in Python, sorting integers as if strings
```
rdd.sortByKey(ascending=True, numPartitions=None, keyfunc = lambda x: str(x))
```



- `countByKey()` Count the number of elements for each key.
- `collectAsMap()`
- `lookup(key)` Return all values associated with the provided
key.


## Data Partitioning
?????









# Chapter 5. Loading and Saving Your Data (106)
### Example
Loading a text file in Python
```
input = sc.textFile("file:///home/holden/repos/spark/README.md")
```

### Example
Saving as a text file in Python
```
result.saveAsTextFile(outputFile)
```

### Example
Loading unstructured JSON in Python
```
import json
data = input.map(lambda x: json.loads(x))
```


### Example
Saving JSON in Python
```
(data.filter(lambda x: x[‘lovesPandas’]).map(lambda x: json.dumps(x))
.saveAsTextFile(outputFile))
```


```
rdd.coalesce(3).map(lambda x: json.dumps(x)).saveAsTextFile('p1')
```

```
df.coalesce(1).write.format('json').save('p2')
```


### Example
Loading CSV with textFile() in Python
```
import csv
import StringIO
… def loadRecord(line):
"""Parse a CSV line"""
input = StringIO.StringIO(line)
reader = csv.DictReader(input, fieldnames=["name", "favouriteAnimal"])
return reader.next()
input = sc.textFile(inputFile).map(loadRecord)
```


### Example
Loading CSV in full in Python
```
def loadRecords(fileNameContents):
"""Load all the records in a given file"""
input = StringIO.StringIO(fileNameContents[1])
reader = csv.DictReader(input, fieldnames=["name", "favoriteAnimal"])
return reader
fullFileData = sc.wholeTextFiles(inputFile).flatMap(loadRecords)
```

### Example
Writing CSV in Python
```
def writeRecords(records):
    """Write out CSV lines"""
    output = StringIO.StringIO()
    writer = csv.DictWriter(output, fieldnames=["name", "favoriteAnimal"])
    for record in records:
        writer.writerow(record)
    return [output.getvalue()]

pandaLovers.mapPartitions(writeRecords).saveAsTextFile(outputFile)
```





# Chapter 6. Advanced Spark Programming (139)
# Chapter 7. Running on a Cluster (157)
# Chapter 8. Tuning and Debugging Spark (189)
# Chapter 9. Spark SQL (214)

Python SQL imports
```
# Import Spark SQL
from pyspark.sql import HiveContext, Row
# Or if you can’t include the hive requirements
from pyspark.sql import SQLContext, Row
```

Constructing a SQL context in Python
```
hiveCtx = HiveContext(sc)
```

Loading and quering tweets in Python
```
input = hiveCtx.jsonFile(inputFile)
# Register the input schema RDD
input.registerTempTable(“tweets”)
# Select tweets based on the retweetCount
topTweets = hiveCtx.sql(“““SELECT text, retweetCount FROM
tweets ORDER BY retweetCount LIMIT 10”””)
```


### Accessing the text column in the topTweets SchemaRDD in Python
```
topTweetText = topTweets.map(lambda row: row.text)
```

### Hive load in Python
```
from pyspark.sql import HiveContext
hiveCtx = HiveContext(sc)
rows = hiveCtx.sql(“SELECT key, value FROM mytable”)
keys = rows.map(lambda row: row[0])
```

### Parquet load in Python
```
# Load some data in from a Parquet file with field’s name and favouriteAnimal
rows = hiveCtx.parquetFile(parquetFile)
names = rows.map(lambda row: row.name)
print “Everyone”
print names.collect()
```


### Parquet query in Python
```
# Find the panda lovers
tbl = rows.registerTempTable(“people”)
pandaFriends = hiveCtx.sql(“SELECT name FROM people WHERE favouriteAnimal = "panda"”)
print “Panda friends”
print pandaFriends.map(lambda row: row.name).collect()
```


### Parquet file save in Python
```
pandaFriends.saveAsParquetFile(“hdfs://…”)
```

### Loading JSON with Spark SQL in Python
```
input = hiveCtx.jsonFile(inputFile)
```


### Creating a SchemaRDD using Row and named tuple in Python
```
happyPeopleRDD = sc.parallelize([Row(name=“holden”, favouriteBeverage=“coffee”)])
happyPeopleSchemaRDD = hiveCtx.inferSchema(happyPeopleRDD)
happyPeopleSchemaRDD.registerTempTable(“happy_people”)
```


### Python string length UDF (User-Defined Functions)
```
# Make a UDF to tell us how long some text is
hiveCtx.registerFunction(“strLenPython”, lambda x: len(x), IntegerType())
lengthSchemaRDD = hiveCtx.sql(“SELECT strLenPython(‘text’) FROM tweets LIMIT 10”)
```

Using a Hive UDF requires that we use the HiveContext instead of a regular SQLContext.
To make a Hive UDF available, simply call `hiveCtx.sql(“CREATE TEMPORARY FUNCTION
name AS class.function“)`.








# Chapter 10. Spark Streaming (243)
# Chapter 11. Machine Learning with MLlib (283)
