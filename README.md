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

## Prerequisites

- Scala 2.13
- sbt 1.3.3
- JVM 8
- Hadoop 3.2

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
- Kafka (local instance)

Execute the test suite via sbt like so.

```bash
$ sbt test
```

Or test only specific suite by entering sbt console.

```bash
$ sbt
sbt> testOnly *Data*
```

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




### Datasets used in samples

All licences and copyrights belong to the original owners of those datasets.

- [Daily Temperature of Major Cities (Kaggle)](https://twitter.com/Pumping_Hard/status/1041736341409587201)

---

## Licence 

Apache licence. Redistribution, modification, private use, sublicencing are permitted.