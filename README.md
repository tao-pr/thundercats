# Thundercats

Write Spark in Functional way.


## Motivation

What if we can write Spark in the following way.

```scala
val p = for {
  a <- IO.Read.parquet("foo.parquet")
  b <- IO.Read.csv("bar.csv", header=True)
  c <- MQ.Read.kafka("topic", limit=1000)
  d <- Join.left(a, b, "col1")
  _ <- IO.Write.parquet(d, "new_foo.parquet")
  e <- Group.agg(d, by="col2", sum("col3").as("t"), avg("col4").as("v"))
} yield e

val q = for {
  a <- p
  _ <- MQ.Write.kafka("topic", a)
  b <- Filter(a, $("col1") > 35)
} yield b.cache
```

## Supported Data Sources

- [x] Physical file types: CSV, Parquet
- [x] Streaming sources: Kafka
- [ ] Relational databases: TBD
- [ ] NoSQL databases: TBD

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

To run a full test on all suites, the following dependencies are required.

- Hadoop
- Kafka

Execute the test suite via sbt like so.

```bash
$ sbt test
```

Or test only specific suite by entering sbt console.

```bash
$ sbt
sbt> testOnly *PipeTest*
```

## Licence 

TBD soon later