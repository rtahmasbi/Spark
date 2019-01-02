
Summary of book
**Learning Spark** by _Holden Karau, Andy Konwinski, Patrick Wendell, and Matei Zaharia_

---




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



# Chapter 1. Introduction to Data Analysis with Spark (15)





**resilient distributed dataset (RDD)**


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



### example
```
from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local").setAppName("My App")
sc = SparkContext(conf = conf)

rdd = sc.textFile("aa.txt")
res1 = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))

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
We introduce two types of shared variables:
**accumulators** to aggregate information and **broadcast** variables to efficiently distribute
large values.


## Accumulators
When we normally pass functions to Spark, such as a `map()` function or a condition for
`filter()`, they can use variables defined outside them in the driver program, but each task
running on the cluster gets a new copy of each variable, and updates from these copies are
not propagated back to the driver. Spark’s shared variables, **accumulators** and **broadcast**
variables, relax this restriction for two common types of communication patterns:
aggregation of results and broadcasts.


### Accumulator empty line count in Python
```
file = sc.textFile(inputFile)
# Create Accumulator[Int] initialized to 0
blankLines = sc.accumulator(0)

def extractCallSigns(line):
    global blankLines # Make the global variable accessible
    if (line == ””):
    blankLines += 1
    return line.split(” “)

callSigns = file.flatMap(extractCallSigns)
callSigns.saveAsTextFile(outputDir + “/callsigns”)
print “Blank lines: %d” % blankLines.value
```

Note that we will see the right count only after we run the `saveAsTextFile()` action, because the transformation above it, `map()`, is lazy, so the side effect incrementing of the accumulator will happen only when the **lazy** `map()` transformation is forced to occur by the `saveAsTextFile()` action.

Of course, it is possible to aggregate values from an entire RDD back to the driver
program using actions like reduce(), but sometimes we need a simple way to aggregate
values that, in the process of transforming an RDD, are generated at different scale or
granularity than that of the RDD itself.



### Accumulator error count in Python
```
# Create Accumulators for validating call signs
validSignCount = sc.accumulator(0)
invalidSignCount = sc.accumulator(0)

def validateSign(sign):
    global validSignCount, invalidSignCount
    if re.match(r”\A\d?[a-zA-Z]{1,2}\d{1,4}[a-zA-Z]{1,3}\Z”, sign):
        validSignCount += 1
        return True
    else:
        invalidSignCount += 1
        return False

# Count the number of times we contacted each call sign
validSigns = callSigns.filter(validateSign)
contactCount = validSigns.map(lambda sign: (sign, 1)).reduceByKey(lambda (x, y): x + y)

# Force evaluation so the counters are populated
contactCount.count()
if invalidSignCount.value < 0.1 * validSignCount.value:
    contactCount.saveAsTextFile(outputDir + “/contactCount”)
else:
    print “Too many errors: %d in %d” % (invalidSignCount.value, validSignCount.value
```



## Custom Accumulators
Spark also includes an API to define custom
accumulator types and custom aggregation operations (e.g., finding the maximum of the
accumulated values instead of adding them).

Beyond adding to
a numeric value, we can use any operation for add, provided that operation is commutative
and associative. For example, instead of adding to track the total we could keep track of
the maximum value seen so far.


## Broadcast Variables
Spark’s second type of shared variable, broadcast variables, allows the program to
efficiently send a large, read-only value to all the worker nodes for use in one or more
Spark operations.


### Country lookup in Python
```
# Look up the locations of the call signs on the
# RDD contactCounts. We load a list of call sign
# prefixes to country code to support this lookup.
signPrefixes = loadCallSignTable()

def processSignCount(sign_count, signPrefixes):
    country = lookupCountry(sign_count[0], signPrefixes)
    count = sign_count[1]
    return (country, count)

countryContactCounts = (contactCounts
    .map(processSignCount)
    .reduceByKey((lambda x, y: x+ y)))


```
This program would run, but if we had a larger table (say, with IP addresses instead of call
signs), the signPrefixes could easily be several megabytes in size, making it expensive
to send that Array from the master alongside each task. In addition, if we used the same
signPrefixes object later (maybe we next ran the same code on file2.txt), it would be sent
again to each node.

### Country lookup with Broadcast values in Python
```
# Look up the locations of the call signs on the
# RDD contactCounts. We load a list of call sign
# prefixes to country code to support this lookup.
signPrefixes = sc.broadcast(loadCallSignTable())

def processSignCount(sign_count, signPrefixes):
    country = lookupCountry(sign_count[0], signPrefixes.value)
    count = sign_count[1]
    return (country, count)

countryContactCounts = (contactCounts
    .map(processSignCount)
    .reduceByKey((lambda x, y: x+ y)))

    countryContactCounts.saveAsTextFile(outputDir + “/countries.txt”)
```
`countryContactCounts.saveAsTextFile` is just for runnig (lazy computing)



## Working on a Per-Partition Basis
Working with data on a per-partition basis allows us to avoid redoing setup work for each
data item. Operations like opening a database connection or creating a random-number
generator are examples of setup steps that we wish to avoid doing for each element. Spark
has per-partition versions of `map` and `foreach` to help reduce the cost of these operations
by letting you run code only once for each partition of an RDD.


### Shared connection pool in Python
```
def processCallSigns(signs):
    “““Lookup call signs using a connection pool”””
    # Create a connection pool
    http = urllib3.PoolManager()
    # the URL associated with each call sign record
    urls = map(lambda x: “http://73s.com/qsos/%s.json” % x, signs)
    # create the requests (non-blocking)
    requests = map(lambda x: (x, http.request(‘GET’, x)), urls)
    # fetch the results
    result = map(lambda x: (x[0], json.loads(x[1].data)), requests)
    # remove any empty results and return
    return filter(lambda x: x[1] is not None, result)

def fetchCallSigns(input):
    “““Fetch call signs”””
    return input.mapPartitions(lambda callSigns : processCallSigns(callSigns))

contactsContactList = fetchCallSigns(validSigns)
```

### Average without mapPartitions() in Python
```
def combineCtrs(c1, c2):
    return (c1[0] + c2[0], c1[1] + c2[1])

def basicAvg(nums):
    “““Compute the average”””
    nums.map(lambda num: (num, 1)).reduce(combineCtrs)
```


### Average with mapPartitions() in Python
```
def partitionCtr(nums):
    “““Compute sumCounter for partition”””
    sumCount = [0, 0]
    for num in nums:
        sumCount[0] += num
        sumCount[1] += 1
    return [sumCount]

def fastAvg(nums):
    “““Compute the avg”””
    sumCount = nums.mapPartitions(partitionCtr).reduce(combineCtrs)
    return sumCount[0] / float(sumCount[1])
```




## Piping to External Programs
### R distance program
```
#!/usr/bin/env Rscript
library(“Imap”)
f <- file(“stdin”)
open(f)
while(length(line <- readLines(f,n=1)) > 0) {
    # process line
    contents <- Map(as.numeric, strsplit(line, “,”))
    mydist <- gdist(contents[[1]][1], contents[[1]][2],
    contents[[1]][3], contents[[1]][4],
    units=“m”, a=6378137.0, b=6356752.3142, verbose = FALSE)
    write(mydist, stdout())
}
```
If that is written to an executable file named ./src/R/finddistance.R, then it looks like this
in use:
```
$ ./src/R/finddistance.R
37.75889318222431,-122.42683635321838,37.7614213,-122.4240097
349.2602
coffee
NA
ctrl-d
```

## Numeric RDD Operations
??

### Removing outliers in Python
```
# Convert our RDD of strings to numeric data so we can compute stats and
# remove the outliers.
distanceNumerics = distances.map(lambda string: float(string))
stats = distanceNumerics.stats()
stddev = std.stdev()
mean = stats.mean()
reasonableDistances = distanceNumerics.filter(
lambda x: math.fabs(x - mean) < 3 * stddev)
print reasonableDistances.collect()
```




# Chapter 7. Running on a Cluster (157)

Spark can run on a wide variety
of cluster managers (Hadoop YARN, Apache Mesos, and Spark’s own built-in Standalone
cluster manager) in both on-premise and cloud deployments.


In distributed mode, Spark uses a master/slave architecture with one central coordinator
and many distributed workers. The central coordinator is called the **driver**. The driver
communicates with a potentially large number of distributed workers called **executors**.
The driver runs in its own Java process and each executor is a separate Java process. A
driver and its executors are together termed a Spark **application**.


A Spark application is launched on a set of machines using an external service called a
**cluster manager**. As noted, Spark is packaged with a built-in cluster manager called the
Standalone cluster manager. Spark also works with Hadoop YARN and Apache Mesos,
two popular open source cluster managers.

## The driver
The driver is the process where the `main()` method of your program runs. It is the process
running the user code that creates a SparkContext, creates RDDs, and performs
transformations and actions.



## Executors
Spark executors are worker processes responsible for running the individual tasks in a
given Spark job. Executors are launched once at the beginning of a Spark application and
typically run for the entire lifetime of an application, though Spark applications can
continue if executors fail. Executors have two roles. First, they run the tasks that make up
the application and return results to the driver. Second, they provide in-memory storage
for RDDs that are cached by user programs, through a service called the Block Manager
that lives within each executor. Because RDDs are cached directly inside of executors,
tasks can run alongside the cached data.

## Cluster Manager
Cluster Manager allows Spark to run on top of different external
managers, such as YARN and Mesos, as well as its built-in Standalone cluster manager.


Spark’s documentation consistently uses the terms `driver` and `executor` when describing the processes that execute each Spark application. The terms `master` and `worker` are used to describe the centralized and distributed portions of the cluster manager.

## Launching a Program

Spark provides a single script you can use to
submit your program to it called `spark-submit`.
```
bin/spark-submit my_script.py
bin/spark-submit —master spark://host:7077 —executor-memory 10g my_script.py
```

`—master` can be:
```
spark://host:port
mesos://host:port # Connect to a Mesos cluster master at the specified port. By default Mesos masters listen on port 5050.
yarn
local
local[N] # Run in local mode with N cores.
local[*] # Run in local mode and use as many cores as the machine has.
```

`-files`:  A list of files to be placed in the working directory of your application. This can be used for data files that
you want to distribute to each node.

`-pyfiles`: A list of files to be added to the PYTHONPATH of your application. This can contain .py, .egg, or .zip files.



### Submitting a Python application in YARN client mode
```
$ export HADOP_CONF_DIR=/opt/hadoop/conf
$ ./bin/spark-submit \
—master yarn \
—py-files somelib-1.2.egg,otherlib-4.4.zip,other-file.py \
—deploy-mode client \
—name “Example Program” \
—queue exampleQueue \
—num-executors 40 \
—executor-memory 10g \
my_script.py “options” “to your application” “go here”
```



## Amazon EC2
Spark comes with a built-in script to launch clusters on Amazon EC2. This script launches
a set of nodes and then installs the Standalone cluster manager on them, so once the
cluster is up, you can use it according to the Standalone mode instructions in the previous
section. In addition, the EC2 script sets up supporting services such as HDFS, Tachyon,
and Ganglia to monitor your cluster.


To launch a cluster, you should first create an Amazon Web Services (AWS) account and obtain an access key ID and secret access key. Then export these as environment
variables:
```
export AWS_ACCESS_KEY_ID=“…”
export AWS_SECRET_ACCESS_KEY=“…”
```

In addition, create an EC2 SSH key pair and download its private key file (usually called
`keypair.pem`) so that you can SSH into the machines.

Next, run the launch command of the `spark-ec2` script, giving it your key pair name, private key file, and a name for the cluster. By default, this will launch a cluster with one master and one slave, using `m1.xlarge` EC2 instances:
```
cd /path/to/spark/ec2
./spark-ec2 -k mykeypair -i mykeypair.pem launch mycluster
```

You can also configure the instance types, number of slaves, EC2 region, and other factors
using options to `spark-ec2`. For example:
```
# Launch a cluster with 5 slaves of type m3.xlarge
./spark-ec2 -k mykeypair -i mykeypair.pem -s 5 -t m3.xlarge launch mycluster
```




## Logging in to a cluster
You can log in to a cluster by SSHing into its master node with the .pem file for your keypair. For convenience, spark-ec2 provides a login command for this purpose:
```
./spark-ec2 -k mykeypair -i mykeypair.pem login mycluster
```
Alternatively, you can find the master’s hostname by running:
```
./spark-ec2 get-master mycluster
```
Then SSH into it yourself using `ssh -i keypair.pem root@masternode`.


To destroy a cluster launched by spark-ec2, run:
```
./spark-ec2 destroy mycluster
```

To stop a cluster, use:
```
./spark-ec2 stop mycluster
```
Then, later, to start it up again:
```
./spark-ec2 -k mykeypair -i mykeypair.pem start mycluster
```


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
