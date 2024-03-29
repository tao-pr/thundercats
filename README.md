# Thundercats

Write Spark in Functional way.

--- 

## Motivation

What if we can write Spark in the following `Monadic` way.

```scala
val p = for {
  a <- IO.Read.parquet("foo.parquet")
  b <- IO.Read.csv("bar.csv", header=True)
  c <- IO.Read.kafka("topic", limit=1000)
  d <- Join.left(a, b, "col1")
  _ <- IO.Write.parquet(d, "new_foo.parquet")
  e <- Group.agg(d, by="col2", sum("col3").as("t"), avg("col4").as("v"))
} yield e

val q = for {
  a <- p
  _ <- IO.Write.kafka("topic", a)
  b <- Filter(a, $("col1") > 35)
} yield b.cache
```

## Supported Data Sources

- [x] Physical file types: CSV, Parquet
- [x] Streaming sources: Kafka
- [x] MongoDB
- [x] Amazon DynamoDB

## Prerequisites

- Scala 2.12
- sbt 1.3.3
- JVM 8
- Hadoop 3.2
- Spark 3.0

## Workaround: Java 8 instead of Java 13, compatibility issue

Spark 2 still has a known issue with Java 13 so it is recommended to 
install and activate Java 8 on the machine. On MacOS, the users can 
do the following. **NOTE** This installation is slow.

```bash
$ brew tap AdoptOpenJDK/openjdk
$ brew cask install adoptopenjdk/openjdk/adoptopenjdk8
```

Then activate the Java8

```bash
$ export JAVA_HOME=$(/usr/libexec/java_home -v 1.8)
```

To switch back the Java13, then

```bash
$ export JAVA_HOME=$(/usr/libexec/java_home -v 13)
```

Note that you can list all installed Javas by

```bash
$ ll /Library/Java/JavaVirtualMachines
```

## Tests

Following dependencies are required to run the test suite.

- Hadoop
- Docker

Run a full unittest

```
./run-test.sh
```

The script starts instances of required components, e.g. DynamoDB, with Docker compose 
and start a unittest normally with `sbt test`. After all tests are done, the script 
also stops the docker instances for you.

---

## Usage

Add `Thundercat` to your project and import the following.

```scala
import com.tao.thundercats.physical._
```

### Read dataframes from files

Currently CSV and parquet are supported

```scala
val df = for {
  a <- Read.csv("path/to/file.csv", withHeader=false, delimiter=";")
  b <- Read.csv("path/to/file.csv")
  c <- Read.parquet("path/to/file.parquet")
  d <- Read.mongo("localhost", "db1", "collection1")
} yield ???
```

### Write to files

```scala
for {
  ...
  a <- Write.csv(df, "path/to/file.csv", withHeader=true, delimiter="\t")
  b <- Write.csv(df, "path/to/file.csv")
  c <- Write.parquet(df, "path/to/file.parquet")
} yield ???
```

### Read and write Kafka

```scala
for {
  ...
  a <- Read.kafkaStream("topic", "server-address", 9092) // Stream
  b <- Read.kafka("topic", "server-address", 9092) // Batch
  c <- Read.kafka("topic", "server-address", colEncoder=ColumnEncoder.Avro(schemaStr))
  ...
  _ <- Write.kafkaStream(dfStream, "topic", "server-address", 9092)
  _ <- Write.kafka(dfBatch, "topic", "server-address", 9092, ColumnEncoder.Avro(schemaStr))
  _ <- Write.kafka(dfBatch, "topic", colEncoder=ColumnEncoder.Avro(schemaStr))
  _ <- Write.kafka(dfBatch, "topic")

  ...
} yield ???
```

### Read from MongoDB

```scala
for {
  ...
  a <- Read.mongo("127.0.0.1", "db-name", "collection-name")
  ...
} yield ???
```

### Show streaming dataframe

```scala
for {
  ...
  a <- Read.kafkaStream("topic", "server-address", 9092) // Stream
  _ <- Screen.showDFStream(a, title=Some("Streaming dataframe"))
}
```

### Show normal dataframe (bounded)

```scala
for {
  ...
  a <- Read.csv("path.csv")
  _ <- Screen.showDF(a)
}
```

### Join, filter, groupby

```scala
for {
  ...
  a <- Read.csv("path")
  b <- Read.parquet("path")
  c <- Read.kafka("topic")
  ...
  f <- Join.outer(a, b, Join.on("key" :: Nil))
  g <- Join.left(a, b, Join.on("key" :: Nil))
  k <- Join.inner(a, b, Join.on("key" :: "key2" :: Nil))
  _ <- Join.inner(a, b, Join.with('key :: 'value * 10 :: Nil)) // Using column objects
  ...
  n <- Group.agg(f, Seq('key, min('value)), Group.map(
    "value" -> "min", 
    "value" -> "avg",
    "n" -> "collect_set"))
  m <- Group.agg(f, Seq('key), Group.agg(min('value), max('value), collect_set('value))
  ...
  q <- Filter.where(n, 'value > 250)
}
yield ???
```

### Apply mapping function: DataFrame => DataFrame

If you have a function of signature `DataFrame => DataFrame`, 
you can also apply it with binding operator as follows.

```scala
for {
  ...
  a <- Read.csv("path")
  b <- Read.parquet("path")
  ...
  h <- b >> (_.withColumn("c", lit(true)))
  _ <- b >> (_.withColumn("d", explode("array")))
}
yield ???

```

### WithColumn equivalence

To add new column into for comprehension, do following

```scala
for {
  ...
  a <- Read.csv("path")
  b <- Read.parquet("path")
  ...
  z <- F.addColumn(a, "new_col", explode('old_col))
  w <- F.addColumn(z, "new_col2", add_months('old_col_m, 4))
}
yield ???
```

---

## Samples

Check out the project "samples" for example usage of Thundercats. 
Build and package as JAR, then submit to your spark cluster of choice.

## Run samples locally

You can try samples on your local workstation too. Follow the instructions below

1. Copy data files into `$HOME/data`

```
mkdir -p $HOME/data
cp samples/src/main/resources/*.csv $HOME/data
```

2. Package JAR

```bash
$ sbt samples/assembly
```

3. Start spark shell with the packaged JAR

```bash
$ $SPARK_HOME/bin/spark-shell --jars samples/target/scala-2.12/samples-assembly-0.1.0-SNAPSHOT.jar
```

4. You can choose to run any sample instances. See following samples.

```bash
scala> import com.tao.thundercats.samples.subapp._

scala> DataPipeline.runMe(spark)
```


### Datasets used in samples

All licences and copyrights belong to the original owners of those datasets.

- [Daily Temperature of Major Cities (Kaggle)](https://twitter.com/Pumping_Hard/status/1041736341409587201)
- [Countries of the world (Kaggle)](https://www.kaggle.com/fernandol/countries-of-the-world)

---

## Licence 

Apache licence. Redistribution, modification, private use, sublicencing are permitted.