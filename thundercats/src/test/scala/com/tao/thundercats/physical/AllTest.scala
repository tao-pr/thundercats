package com.tao.thundercats.physical

import com.tao.thundercats.base.SparkStreamTestInstance
import com.tao.thundercats.estimator._
import com.tao.thundercats.evaluation._
import com.tao.thundercats.functional._
import com.tao.thundercats.model._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.scalatest.Inspectors._
import org.scalatest.{Filter => _}

import java.io.File
import scala.sys.process._

import org.scalatest.{Filter => _, _}
import matchers.should._

object IO {
  def getFile(path: String): Option[File] = {
    val f = new File(path)
    if (f.exists) Some(f) else None
  }

  def delete(file: File): Option[Boolean] = Some(file.delete)

  def deleteFiles(files: List[String]) = {
    files.foreach{ f => 
      for {
        w <- IO.getFile(f);
        _ <- IO.delete(w) }
        yield true
    }
  }

  def cleanupCheckpointDirs() {
    import scala.reflect.io.Directory
    Seq("./chk", "./chk3", "./chk_parq", "./out_parquet").foreach{ d =>
      new Directory(new File(d)).deleteRecursively()
    }
  }
}


case class A(i: Int, s: Option[String])
case class K(key: String, value: String)
case class Kx(key: String, value: String, b: Int)

case class Train(i: Int, d: Double, v: Double, w: Double, s: String, s2: String)

case class W(i: Int, d: Double, label: Double)

class DataSuite extends SparkStreamTestInstance with Matchers {

  import spark.implicits._

  describe("Basic IO"){

    lazy val tempCSV = File.createTempFile("tc-test-", ".csv").getAbsolutePath
    lazy val tempParquet = File.createTempFile("tc-test-", ".parquet").getAbsolutePath

    val topic = "topic-1"
    val topic2 = "topic-2"
    val topic3 = "topic-3"
    val serverAddr = "localhost"
    val serverPort = 29092

    lazy val df = List(
      A(1, None),
      A(2, Some("foo")),
      A(3, Some("bar"))
    ).toDS.toDF

    lazy val dfK = List(
      K("foo1", "bar-01"),
      K("foo2", "bar-02"),
      K("foo3", "bar-03")
    ).toDS.toDF

    it("PRE: cleanup tempfiles"){
      IO.deleteFiles(tempCSV :: tempParquet :: Nil)
    }

    it("PRE: cleanup checkpoint directories"){
      IO.cleanupCheckpointDirs()
    }

    it("PRE: flush Kafka"){
      Seq(topic, topic2, topic3).foreach{ top => 
        val cmd = s"kafka-topics --bootstrap-server ${serverAddr}:${serverPort} --topic ${top} --delete"
        Console.println(Console.YELLOW + s"Executing ${cmd}" + Console.RESET)
        cmd !
      }
    }

    it("PRE: Create steaming topic (2)"){
      val cmd = s"kafka-topics --create --partitions 1 --replication-factor 1 --topic ${topic2} --bootstrap-server ${serverAddr}:${serverPort}"
      Console.println(Console.YELLOW + s"Creating streaming topic : ${topic2}" + Console.RESET)
      cmd !
    }

    it("PRE: Create steaming topic (3)"){
      val cmd = s"kafka-topics --create --partitions 1 --replication-factor 1 --topic ${topic3} --bootstrap-server ${serverAddr}:${serverPort}"
      Console.println(Console.YELLOW + s"Creating streaming topic : ${topic3}" + Console.RESET)
      cmd !
    }

    it("write and read csv"){
      val dfReadOpt = for { 
        b <- Write.csv(df, tempCSV)
        c <- Read.csv(tempCSV)
      } yield c

      val dfRead = dfReadOpt.get

      dfRead.count shouldBe (df.count)
      dfRead.map(_.getAs[Int]("i")).collect shouldBe (Seq(1,2,3))
    }

    it("fails to read csv which does not exist"){
      val dfReadOpt = for { 
        c <- Read.csv("./not-found.csv")
      } yield c

      dfReadOpt.getError.map(_.contains("checkAndGlobPathIfNecessary") shouldBe true)
      dfReadOpt.isFailing shouldBe true
    }

    it("rename columns"){
      val dfRenamedOpt = for {
        c <- Transform.rename(df, Map(
          "i" -> "iii"
        ))
      } yield c

      dfRenamedOpt.isFailing shouldBe false
      val dfRenamed = dfRenamedOpt.get

      dfRenamed.columns shouldBe List("iii", "s")
    }

    it("write and read parquet"){
      val dfReadOpt = for { 
        b <- Write.parquet(df, tempParquet)
        c <- Read.parquet(tempParquet)
      } yield c

      val dfRead = dfReadOpt.get

      dfRead.count shouldBe (df.count)
      dfRead.map(_.getAs[Int]("i")).collect shouldBe (Seq(1,2,3))
    }

    it("write and read Kafka (batch)"){
      val dfReadOpt = for { 
        b <- Write.kafka(dfK, topic, serverAddr, serverPort)
        c <- Read.kafka(topic, serverAddr, serverPort)
      } yield c

      val dfRead = dfReadOpt.get

      dfRead.count shouldBe (dfK.count)
      dfRead.map(_.getAs[String]("key")).collect shouldBe (Seq("foo1", "foo2", "foo3"))
    }

    ignore("read from dynamoDB"){
      val dfReadOpt = for {
        a <- Read.dynamo("eu-central-1", "0.0.0.0:8000", "Entry")
      } yield a

      dfReadOpt match {
        case Ok(df) => 
          df.show()
        case Fail(e) =>
          e.toString shouldBe "" // Just always fails and shows full stacktrace
      }
    }

    it("POST: cleanup tempfiles"){
      IO.deleteFiles(tempCSV :: tempParquet :: Nil)
    }

    it("POST: flush Kafka"){
      Seq(topic, topic2, topic3).foreach{ top => 
        s"kafka-topics --bootstrap-server ${serverAddr}:${serverPort} --topic ${top} --delete" !
      }
    }

    it("POST: cleanup checkpoint directories"){
      IO.cleanupCheckpointDirs()
    }
  }

  describe("Basic estimators"){

    import spark.implicits._

    lazy val dfK1 = List(
      K("a", "111"),
      K("b", "222"),
      K("c", "333"),
      K("d", "444")
    ).toDS.toDF

    it("rename column"){
      val t = new ColumnRename().setInputCol("key").setOutputCol("k")
      val d = t.fit(dfK1).transform(dfK1)

      d.columns shouldBe Seq("k","value")
    }
  }

  describe("Basic operations"){

    import spark.implicits._

    lazy val dfK1 = List(
      K("a", "111"),
      K("b", "222"),
      K("c", "333"),
      K("d", "444")
    ).toDS.toDF.withColumnRenamed("value", "v1")

    lazy val dfK2 = List(
      K("a", "a1"),
      K("a", "a2"),
      K("c", "c1"),
      K("d", "d1"),
      K("d", "d2"),
      K("e", "e1")
    ).toDS.toDF.withColumnRenamed("value", "v2")

    lazy val dfK3 = List(
      Kx("a", "111", 1),
      Kx("a", "111", 2),
      Kx("c", "333", 1),
      Kx("d", "444", 1),
      Kx("d", "444", 2),
      Kx("d", "444", 3)
    ).toDS.toDF

    it("Left join"){
      import dfK1.sqlContext.implicits._
      val dfOpt = for {
        a <- Join.left(dfK1, dfK2, Join.On("key" :: Nil))
      } yield a

      dfOpt.get.columns shouldBe (Seq("key", "v1", "v2"))
      dfOpt.get.map{ row => (
        row.getAs[String]("key"),
        row.getAs[String]("v1"),
        row.getAs[String]("v2")
      )}.collect.toSet shouldBe (
          Set(
            ("a", "111", "a1"),
            ("a", "111", "a2"),
            ("b", "222", null),
            ("c", "333", "c1"),
            ("d", "444", "d1"),
            ("d", "444", "d2")
          )
        )
    }

    it("Inner join"){
      import dfK1.sqlContext.implicits._
      val dfOpt = for {
        a <- Join.inner(dfK1, dfK2, Join.On("key" :: Nil))
      } yield a

      dfOpt.get.columns shouldBe (Seq("key", "v1", "v2"))
      dfOpt.get.map{ row => (
        row.getAs[String]("key"),
        row.getAs[String]("v1"),
        row.getAs[String]("v2")
      )}.collect.toSet shouldBe (
          Set(
            ("a", "111", "a1"),
            ("a", "111", "a2"),
            ("c", "333", "c1"),
            ("d", "444", "d1"),
            ("d", "444", "d2")
          )
        )
    }

    it("Outer join"){
      import dfK1.sqlContext.implicits._
      val dfOpt = for {
        a <- Join.outer(dfK1, dfK2, Join.On("key" :: Nil))
      } yield a

      dfOpt.get.columns shouldBe (Seq("key", "v1", "v2"))
      dfOpt.get.map{ row => (
        row.getAs[String]("key"),
        row.getAs[String]("v1"),
        row.getAs[String]("v2")
      )}.collect.toSet shouldBe (
          Set(
            ("a", "111", "a1"),
            ("a", "111", "a2"),
            ("b", "222", null),
            ("c", "333", "c1"),
            ("d", "444", "d1"),
            ("d", "444", "d2"),
            ("e", null, "e1")
          )
        )
    }

    it("Broadcast join"){ // As left join
      import dfK1.sqlContext.implicits._
      val dfOpt = for {
        a <- Join.broadcast(dfK1, dfK2, "key" :: Nil, "v2" :: Nil)
      } yield a

      dfOpt.get.columns shouldBe (Seq("key", "v1", "v2"))
      dfOpt.get.map{ row => (
        row.getAs[String]("key"),
        row.getAs[String]("v1"),
        row.getAs[String]("v2")
      )}.collect.toSet shouldBe (
          Set(
            ("a", "111", "a1"),
            ("a", "111", "a2"),
            // ("b", "222", null),
            ("c", "333", "c1"),
            ("d", "444", "d1"),
            ("d", "444", "d2")
          )
        )
    }

    it("Broadcast join, multiple keys"){ // As left join
      import dfK1.sqlContext.implicits._
      val dfOpt = for {
        b <- Ok(dfK1.withColumnRenamed("v1", "value"))
        a <- Join.broadcast(b, dfK3, Seq("key", "value"), "b" :: Nil)
      } yield a

      dfOpt.get.columns shouldBe (Seq("key", "value", "b"))
      dfOpt.get.map{ row => (
        row.getAs[String]("key"),
        row.getAs[String]("value"),
        row.getAs[Int]("b")
      )}.collect.toSet shouldBe (
          Set(
            ("a", "111", 1),
            ("a", "111", 2),
            ("c", "333", 1),
            ("d", "444", 1),
            ("d", "444", 2),
            ("d", "444", 3)
          )
        )
    }

    it("Group and aggregate dataframes"){
      import dfK1.sqlContext.implicits._
      val dfOpt = for {
        a <- Join.outer(dfK1, dfK2, Join.On("key" :: Nil))
        b <- Group.agg(a, 'key :: Nil, Group.Map("v1" -> "min", "v2" -> "max"))
      } yield b

      dfOpt.get.columns shouldBe (Seq("key", "min(v1)", "max(v2)"))
      dfOpt.get.map{ row => (
        row.getAs[String]("key"),
        row.getAs[String]("min(v1)"),
        row.getAs[String]("max(v2)")
      )}.collect.toSet shouldBe (
          Set(
            ("a", "111", "a2"),
            ("b", "222", null),
            ("c", "333", "c1"),
            ("d", "444", "d2"),
            ("e", null, "e1")
          )
        )
    }

    it("Filter dataframes"){
      import dfK1.sqlContext.implicits._
      val dfOpt = for {
        a <- Join.outer(dfK1, dfK2, Join.On("key" :: Nil))
        b <- Group.agg(a, 'key :: Nil, Group.Map("v1" -> "min", "v2" -> "max"))
        c <- Filter.where(b, 'key <= "c")
      } yield c

      dfOpt.get.columns shouldBe (Seq("key", "min(v1)", "max(v2)"))
      dfOpt.get.map{ row => (
        row.getAs[String]("key"),
        row.getAs[String]("min(v1)"),
        row.getAs[String]("max(v2)")
      )}.collect.toSet shouldBe (
          Set(
            ("a", "111", "a2"),
            ("b", "222", null),
            ("c", "333", "c1"),
            //("d", "444", "d2"),
            //("e", null, "e1")
          )
        )
    }

    it("Add column"){
      val dfOpt = for {
        a <- F.addCol(dfK1, "b", when('v1==="222", lit(null)).otherwise(sequence(lit(0), lit(5), lit(1))))
      } yield a

      dfOpt.get.columns shouldBe (Seq("key", "v1", "b"))
      dfOpt.get.map{ row => (
        row.getAs[String]("key"),
        row.getAs[String]("v1"),
        row.getAs[Seq[Int]]("b")
      )}.collect.toSet shouldBe (
          Set(
            ("a", "111", Seq(0,1,2,3,4,5)),
            ("b", "222", null),
            ("c", "333", Seq(0,1,2,3,4,5)),
            ("d", "444", Seq(0,1,2,3,4,5))
          )
        )
    }

  }

  describe("Util test"){

    import Implicits._
    import spark.implicits._

    lazy val dfA = List(
      A(1, Some("aa")),
      A(2, Some("bb")),
      A(3, None),
      A(4, None),
      A(5, Some("cc")),
      A(6, Some("")),
      A(7, Some("")),
      A(8, None),
      A(9, Some("dd"))
    ).toDS.toDF

    it("get schema map"){
      val m = dfA.schemaMap
      m shouldBe(Map("i" -> IntegerType, "s" -> StringType))
    }

    it("bind"){
      import spark.implicits._
      val dfOpt = for {
        a <- Filter.where(dfA, 's.isNotNull && 's =!= "")
        b <- a >> (_.withColumn("c", lit("0")))
      } yield b

      dfOpt.get.schemaMap shouldBe (Map("i" -> IntegerType, "s" -> StringType, "c" -> StringType))
      dfOpt.get.map{ row => (
        row.getAs[Int]("i"),
        row.getAs[String]("s"),
        row.getAs[String]("c"))
      }.collect.toSet shouldBe ( Set(
        (1, "aa", "0"),
        (2, "bb", "0"),
        (5, "cc", "0"),
        (9, "dd", "0")
      ))
    }

    it("Filter by na"){
      import spark.implicits._
      val dfOpt = for {
        a <- Filter.na(dfA, Seq("i", "s"))
      } yield a

      dfOpt.get.map{_.getAs[String]("s")}.collect shouldBe List("aa","bb","cc","","","dd")
    }

    it("Filter by range"){
      import spark.implicits._
      val dfOpt = for {
        a <- Filter.byRange(dfA, "i", (3,5))
      } yield a

      dfOpt.get.map{_.getAs[Int]("i")}.collect shouldBe List(3,4,5)
    }
  }

  describe("Agg test"){

    import spark.implicits._

    // Kx(key: String, value: String, b: Int)
    lazy val raw = List(
      Kx("key1", "a", 3),
      Kx("key1", "a", 0),
      Kx("key1", "b", 5),
      Kx("key1", "b", 2),
      Kx("key1", "b", 1),
      Kx("key2", "a", 3),
      Kx("key2", "a", 0),
      Kx("key2", "a", 10),
      Kx("key2", "a", 20),
      Kx("key2", "a", 9),
      Kx("key2", "b", 30)
    )
    lazy val df = raw.toDS.toDF

    it("should aggregate column"){
      val sumOpt = Agg.on(df, "b", (a:Int, b:Int) => a+b )
      sumOpt.get shouldBe raw.map(_.b).sum

      val maxOpt = Agg.on(df, "b", (a:Int, b:Int) => scala.math.max(a,b) )
      maxOpt.get shouldBe raw.map(_.b).max
    }

    it("should aggregate by key"){
      val agg = (a: Int, b: Int) => a+b
      val sumOpt = Agg.byKeyAsRDD[String, Int](df, "key", "b", agg)
      sumOpt.map(_.sortBy{ case(k,v) => k }.collect).get shouldBe List(
        ("key1", raw.filter(_.key=="key1").map(_.b).sum),
        ("key2", raw.filter(_.key=="key2").map(_.b).sum)
      )
    }
  }

  describe("Optimisation test"){

    import spark.implicits._

    lazy val df = List(
      A(1, Some("aa")),
      A(2, Some("bb")),
      A(3, None),
      A(4, None),
      A(5, Some("cc")),
      A(6, Some("")),
      A(7, Some("")),
      A(8, None),
      A(9, Some("dd"))
    ).toDS

    it("snapshot a dataframe"){
      val snap = Optimise.snapshot(df.toDF, "./out_parquet")
        .get
        .where('s.isNotNull && 's.notEqual(""))
        .map(_.getAs[String]("s"))
        .collect

      snap shouldBe List("aa","bb","cc","dd")
    }

    it("materialise a dataframe"){
      val dfFinal = Optimise.materialise(df.toDF).get
      dfFinal.schema.toList shouldBe (df.schema.toList)
    }

    it("PRE: cleanup checkpoint directories"){
      IO.cleanupCheckpointDirs()
    }

  }

  describe("Pipe test"){

    lazy val pipeComplete = new Pipeline().setStages(Array(
      new HashingTF().setInputCol("aa"),
      new VectorAssembler().setInputCols(Array("aa","bb","cc")).setOutputCol("vv"),
      new KMeans().setFeaturesCol("vv")
    ))

    it("add a stage, finds estimator"){
      val pipe = for {
        p <- Pipe.add(pipeComplete, new PCA())
        p_ <- Pipe.estimator(p)
      } yield p_

      val pipeStages = pipe.get.getStages.map(_.getClass.getName)
      pipeStages.length shouldBe 1 // Only the last is taken, regardless of multitude
      pipeStages shouldBe List("org.apache.spark.ml.feature.PCA")
    }

    it("take only transformers"){
      val pipe = for {
        p <- Pipe.withoutEstimator(pipeComplete)
      } yield p

      val pipeStages = pipe.get.getStages.map(_.getClass.getName)
      pipeStages shouldBe List(
        "org.apache.spark.ml.feature.HashingTF", 
        "org.apache.spark.ml.feature.VectorAssembler")
    }
  }

  describe("Feature engineering test"){

    import Implicits._
    import spark.implicits._

    lazy val dfTrain = List(
      Train(1, 0.0, 1.0, -1.0, "foo bar", ""),
      Train(2, 0.1, 2.0, -2.0, "foo baz", "more"),
      Train(3, 1.3, 4.0, 2.0, "zoo bar", "longer"),
      Train(4, 0.1, 2.5, 5.0, "bar baz bar", ""),
      Train(5, 0.5, 0.5, 1.0, "foo bar bar", "more")
    ).toDF

    it("normalise numbers with Scaler"){
      val pipe = new Pipeline().setStages(
        Array(Features.scaleNumbers(dfTrain, normalised=true, logScale=false))
      ).fit(dfTrain)

      val out = pipe.transform(dfTrain)

      out.schemaMap shouldBe Map(
        "i" -> IntegerType,
        "d" -> DoubleType,
        "v" -> DoubleType,
        "w" -> DoubleType,
        "s" -> StringType,
        "s2" -> StringType
      )

      out.rdd.map(_.getAs[Int]("i")).collect shouldBe List(1,2,3,4,5)
      out.rdd.map(_.getAs[Double]("d")).collect shouldBe List(0.0, 0.05, 0.65, 0.05, 0.25)
      out.rdd.map(_.getAs[Double]("v")).collect shouldBe List(0.1, 0.2, 0.4, 0.25, 0.05)
      out.rdd.map(_.getAs[Double]("w")).collect shouldBe List(-0.2, -0.4, 0.4, 1.0, 0.2)
    }

    it("convert to log scale with Scaler"){
      val pipe = new Pipeline().setStages(
        Array(Features.scaleNumbers(dfTrain, normalised=false, logScale=true))
      ).fit(dfTrain)

      val out = pipe.transform(dfTrain)

      out.schemaMap shouldBe Map(
        "i" -> IntegerType,
        "d" -> DoubleType,
        "v" -> DoubleType,
        "w" -> DoubleType,
        "s" -> StringType,
        "s2" -> StringType
      )

      // NOTE: Scala's log(-x) results in 0 instead of NaN
      //       Also with log(0) results in 0 instead of [[Double.Infinity]]
      out.rdd.map(_.getAs[Int]("i")).collect shouldBe List(1,2,3,4,5)
      out.rdd.map(_.getAs[Double]("d")).collect shouldBe List(0, -2.3025850929940455, 0.26236426446749106, -2.3025850929940455, -0.6931471805599453)
      out.rdd.map(_.getAs[Double]("v")).collect shouldBe List(0, 0.6931471805599453, 1.3862943611198906, 0.9162907318741551, -0.6931471805599453)
      out.rdd.map(_.getAs[Double]("w")).collect shouldBe List(0, 0, 0.6931471805599453, 1.6094379124341003, 0.0)
    }

    it("normalise and convert numbers to logscale with Scaler"){
      val pipe = new Pipeline().setStages(
        Array(Features.scaleNumbers(dfTrain, normalised=true, logScale=true))
      ).fit(dfTrain)

      val out = pipe.transform(dfTrain)

      out.schemaMap shouldBe Map(
        "i" -> IntegerType,
        "d" -> DoubleType,
        "v" -> DoubleType,
        "w" -> DoubleType,
        "s" -> StringType,
        "s2" -> StringType
      )

      out.rdd.map(_.getAs[Int]("i")).collect shouldBe List(1,2,3,4,5)
      out.rdd.map(_.getAs[Double]("d")).collect shouldBe List(0.0, -2.995732273553991, -0.4307829160924542, -2.995732273553991, -1.3862943611198906)
      out.rdd.map(_.getAs[Double]("v")).collect shouldBe List(-2.3025850929940455, -1.6094379124341003, -0.916290731874155, -1.3862943611198906, -2.995732273553991)
      out.rdd.map(_.getAs[Double]("w")).collect shouldBe List(0, 0, -0.916290731874155, 0, -1.6094379124341003)
    }

    it("standardise numbers so they have zero mean and unit variance"){
      val pipe = new Pipeline().setStages(
        Array(Features.standardiseNumbers(dfTrain))
      ).fit(dfTrain)

      val out = pipe.transform(dfTrain)

      out.schemaMap shouldBe Map(
        "i" -> IntegerType,
        "d" -> DoubleType,
        "v" -> DoubleType,
        "w" -> DoubleType,
        "s" -> StringType,
        "s2" -> StringType
      )

      List("d","v","w").foreach{ c =>
        val vector = out.rdd.map(_.getAs[Double](c)).collect
        val mean = (vector.sum / vector.length.toDouble)
        
        val beCloseTo1 = be >= 0.99999 and be <= 1.00001
        val beCloseToZero = be <= 1e-6

        mean should beCloseToZero // zero mean
        (vector.map(v => (v - mean)*(v-mean)).sum / vector.length) should beCloseTo1 // unit var
      }

      out.rdd.map(_.getAs[Int]("i")).collect shouldBe List(1,2,3,4,5)
    }

    it("encode strings with StringEncoder (Murmur Hashing)"){
      val NUM_DISTINCT_VALUES = 4
      val pipe = new Pipeline().setStages(
        Array(Features.encodeStrings(dfTrain, encoder=Murmur, suffix="_1"))
      ).fit(dfTrain)

      val out = pipe.transform(dfTrain)

      out.schemaMap shouldBe Map(
        "i" -> IntegerType,
        "d" -> DoubleType,
        "v" -> DoubleType,
        "w" -> DoubleType,
        "s" -> StringType,
        "s2" -> StringType,
        "s_1" -> VectorType,
        "s2_1" -> VectorType
      )

      // All arrays should have the desired fixed length
      out.rdd.map(_.getAs[DenseVector]("s_1")).collect.exists(_.size != NUM_DISTINCT_VALUES) shouldBe false
    }

    // NOTE: Will fail when test locally on laptop
    ignore("encode strings with StringEncoder (TFIDF)"){
      val pipe = new Pipeline().setStages(
        Array(Features.encodeStrings(dfTrain, encoder=TFIDF(minFreq=0), suffix="_feat"))
      ).fit(dfTrain)

      val out = pipe.transform(dfTrain)

      val schema = out.schemaMap
      schema("s") shouldBe ArrayType(StringType,true) // Original columns should also be tokenised and replaced
      schema("s2") shouldBe ArrayType(StringType,true)
      schema("s_feat").toString.contains("VectorUDT") shouldBe true
      schema("s2_feat").toString.contains("VectorUDT") shouldBe true
    }
  }

  describe("Regression Modeling test"){

    import spark.implicits._

    lazy val df = List(
      // i, d, label
      W(0, 0, label=0),
      W(1, 1, label=1),
      W(2, 1, label=2),
      W(3, 1, label=3),
      W(4, 1, label=4)
    ).toDS.toDF

    it("Measure RMSE"){
      val spec = DummySpecimen(Feature("i"), outputCol="d", labelCol="i")
      val score = spec.score(df, RMSE)
      score shouldBe Ok(scala.math.sqrt(2.8))
    }

    it("Measure MAE"){
      val spec = DummySpecimen(Feature("i"), outputCol="d", labelCol="i")
      val score = spec.score(df, MAE)
      score shouldBe Ok(1.2)
    }

    it("Find the best model by measures (dummy + MAE)"){
      val candidates = List(
        Feature("i"),
        Feature("d")
      )
      val design = DummyModelDesign(labelCol="label")
      val (bestScore, bestCol, bestSpec) = new RegressionFeatureCompare(MAE)
        .bestOf(design, candidates, df)
        .get

      bestCol.colName shouldBe "i"
      bestSpec.isInstanceOf[DummySpecimen] shouldBe true
      bestSpec.asInstanceOf[DummySpecimen].featureCol.colName shouldBe "i"
    }

    it("Find the best model by measures (dummy + Pearson)"){
      val candidates = List(
        Feature("i"),
        Feature("d")
      )
      val design = DummyModelDesign(labelCol="label")
      val (bestScore, bestCol, bestSpec) = new RegressionFeatureCompare(PearsonCorr)
        .bestOf(design, candidates, df)
        .get

      bestCol.colName shouldBe "i"
      bestSpec.isInstanceOf[DummySpecimen] shouldBe true
      bestSpec.asInstanceOf[DummySpecimen].featureCol.colName shouldBe "i"
    }

    it("Measure z-score from model design (vector measure)"){
      val df_ = df.withColumn("label-lg", 'label + lit(0.01)*'i)
      val candidates = Array("i","d")
      val features = AssemblyFeature(candidates, "features")
      val design = SupervisedModelDesign(
        outputCol="z",
        labelCol="label-lg",
        estimator=Preset.linearReg(features, "label-lg", "z"))

      val (bestScore, bestCol, bestSpec) = RegressionFeatureCompareVector(ZScore)
        .bestOf(design, candidates.map(Feature.apply), df_.toDF)
        .get

      bestCol shouldBe Feature("i")
      bestSpec.isInstanceOf[SupervisedSpecimen] shouldBe true

      // When measuring vector of features, 
      // the feature cols of specimen will not be the best one, but all in feature vector
      bestSpec.asInstanceOf[SupervisedSpecimen].featureCol.colName shouldBe "features"
    }
  }

  describe("Classification modeling test"){
    import spark.implicits._

    lazy val df = List(
      // i, d, label
      W(0, 0.0, label=0),
      W(1, 0.1, label=0),
      W(0, 1.5, label=1),
      W(1, 1.6, label=1),
      W(1, 1.3, label=1)
    ).toDS.toDF

    it("Measure Precision"){

      // 2020-08-08 10:16:45,712 INFO  - PRECISION : (1.6,1.0)
      // 2020-08-08 10:16:45,712 INFO  - PRECISION : (1.6,1.0)
      // 2020-08-08 10:16:45,714 INFO  - PRECISION : (1.5,0.5)
      // 2020-08-08 10:16:45,714 INFO  - PRECISION : (1.5,0.5)
      // 2020-08-08 10:16:45,714 INFO  - PRECISION : (1.3,0.6666666666666666)
      // 2020-08-08 10:16:45,714 INFO  - PRECISION : (1.3,0.6666666666666666)
      // 2020-08-08 10:16:45,714 INFO  - PRECISION : (0.1,0.75)
      // 2020-08-08 10:16:45,714 INFO  - PRECISION : (0.1,0.75)
      // 2020-08-08 10:16:45,714 INFO  - PRECISION : (0.0,0.6)
      // 2020-08-08 10:16:45,714 INFO  - PRECISION : (0.0,0.6)

      val spec = DummySpecimen(Feature("i"), outputCol="d", labelCol="i")
      val scoreMap = spec.scoreMap(df, Precision).get
      scoreMap shouldNot contain (Double.MinValue)
      scoreMap.get(0.0) shouldBe Some(0.6)
      scoreMap.get(0.1) shouldBe Some(0.75)
      scoreMap.get(1.3) shouldBe Some(0.6666666666666666)
      scoreMap.get(1.5) shouldBe Some(0.5)
      scoreMap.get(1.6) shouldBe Some(1.0)
    }

    it("Measure Recall"){

      val spec = DummySpecimen(Feature("i"), outputCol="d", labelCol="i")
      val scoreMap = spec.scoreMap(df, Recall).get
      scoreMap shouldNot contain (Double.MinValue)
      scoreMap.get(0.0) shouldBe Some(1.0)
      scoreMap.get(0.1) shouldBe Some(1.0)
      scoreMap.get(1.3) shouldBe Some(0.6666666666666666)
      scoreMap.get(1.5) shouldBe Some(0.3333333333333333)
      scoreMap.get(1.6) shouldBe Some(0.3333333333333333)
    }

    it("Measure AUC"){

      val spec = DummySpecimen(Feature("i"), outputCol="d", labelCol="i")
      val score = spec.score(df, AUC).get
      score shouldBe 0.6666666666666666
    }

    it("Run SVM in wrapped estimator"){
      val features = AssemblyFeature("i" :: "d" :: Nil)
      val labelCol = "label"
      val outputCol = "pred"
      val svm = Preset.svm(features, labelCol, outputCol)
      val design = SupervisedModelDesign(
        outputCol,
        labelCol,
        svm)
      val spec = design.toSpecimen(features, df)
      val score = spec.score(df, AUCPrecisionRecall).get
      score should be > 0.5
    }
  }

  describe("Regression Model selector test"){

    lazy val dfPreset = List(
      Train(i=1, d=1.0, v=1.2, w=0.0, s="", s2=""),
      Train(i=2, d=2.0, v=1.5, w=0.0, s="", s2=""),
      Train(i=3, d=3.0, v=2.2, w=0.0, s="", s2=""),
      Train(i=4, d=4.0, v=3.2, w=0.0, s="", s2=""),
      Train(i=5, d=5.0, v=4.2, w=0.0, s="", s2=""),
      Train(i=6, d=6.0, v=5.0, w=0.0, s="", s2=""),
    ).toDS.toDF

    it("generate model specimens from feature combinations"){

      val df = dfPreset.withColumn("u", lit(-1)*col("i"))
      val selector = new FeatureAssemblyGenerator(
        minFeatureCombination=1,
        maxFeatureCombination=3,
        ignoreCols=List("w"))

      val pipe = Preset.linearReg(Feature("features"), "i", "z")
      val combinations = selector.genCombinations(pipe, df)

      val features = combinations.map(_.asArray)

      // Should not include "w" as excluded
      val expectedCombinations = Vector(
        Array("i"), Array("d"), Array("v"), Array("u"), 
        Array("i", "d"), Array("i", "v"), Array("i", "u"), 
        Array("d", "v"), Array("d", "u"), Array("v", "u"), 
        Array("i", "d", "v"), Array("i", "d", "u"), 
        Array("i", "v", "u"), Array("d", "v", "u"))

      expectedCombinations.foreach(comb => features should contain (comb))
    }

    it("find the best feature combinations"){
      val df = dfPreset.withColumn("u", lit(-1)*col("i"))
      val selector = new FeatureAssemblyGenerator(
        minFeatureCombination=1,
        maxFeatureCombination=3,
        ignoreCols=List("i"))

      val estimator = Preset.linearReg(Feature("features"), "i", "z")
      val combinations = selector.genCombinations(estimator, df)
      val design = SupervisedModelDesign(
        outputCol="z",
        labelCol="i",
        estimator=estimator)

      // Measure feature combinations with MAE
      val results = new RegressionFeatureCompare(MAE)
        .allOf(design, combinations, df.toDF)

      val (bestScore, bestCol, bestSpec) = new RegressionFeatureCompare(MAE)
        .bestOf(design, combinations, df.toDF)
        .get

      results.size shouldBe (combinations.size)

      // The best model should be the one with least MAE
      val minScore = results.map{ case(score,_) => score }.min
      bestScore shouldBe minScore
    }

    it("Evaluate all models with ModelCompare"){
      val measure = MAE
      val feat = AssemblyFeature("v"::Nil, "features")

      val df = dfPreset.withColumn("i2", col("i")+col("d"))

      val allModels = List(
        SupervisedModelDesign(
          outputCol="z",
          labelCol="i",
          estimator=Preset.linearReg(features=feat, labelCol="i", outputCol="z")),
        SupervisedModelDesign(
          outputCol="z",
          labelCol="i2",
          estimator=Preset.linearReg(features=feat, labelCol="i", outputCol="z", elasticNetParam=Some(0.01)))
      )
      val allScores = new RegressionModelCompare(measure, feat)
        .allOf(df, allModels)

      allScores.size shouldBe (allModels.size)
      allScores.map{ case(score, m) => m.getClass.getName } shouldBe List(
        "com.tao.thundercats.evaluation.SupervisedSpecimen",
        "com.tao.thundercats.evaluation.SupervisedSpecimen")
      allScores.map{ case(score, m) => score } shouldBe List(0.21092959375451714, 3.4999999999999996)
    }
  }

  describe("Clustering model test"){
    lazy val dfPreset = List(
      // Group1
      Train(i=1, d=1.0, v=1001, w=0, s="", s2=""),
      Train(i=1, d=1.0, v=1000, w=0, s="", s2=""),
      Train(i=1, d=1.0, v=1000, w=0, s="", s2=""),
      Train(i=1, d=1.1, v=1001, w=0, s="", s2=""),
      Train(i=1, d=1.1, v=1000, w=0, s="", s2=""),
      Train(i=1, d=1.0, v=1000, w=0, s="", s2=""),
      Train(i=1, d=1.0, v=1000, w=0, s="", s2=""),
      Train(i=1, d=1.0, v=1001, w=0, s="", s2=""),
      Train(i=1, d=1.0, v=1000, w=0, s="", s2=""),
      Train(i=1, d=1.0, v=1000, w=0, s="", s2=""),
      Train(i=1, d=1.1, v=1001, w=0, s="", s2=""),
      // Group2
      Train(i=5, d=5.0, v=160113, w=100, s="", s2=""),
      Train(i=5, d=5.0, v=160103, w=100, s="", s2=""),
      Train(i=5, d=5.0, v=160103, w=100, s="", s2=""),
      Train(i=5, d=5.1, v=160103, w=125, s="", s2=""),
      Train(i=5, d=5.0, v=160103, w=100, s="", s2=""),
      Train(i=5, d=5.0, v=160103, w=100, s="", s2=""),
      Train(i=5, d=5.0, v=160113, w=100, s="", s2=""),
      Train(i=5, d=5.0, v=160103, w=109, s="", s2=""),
      Train(i=5, d=5.0, v=160103, w=110, s="", s2=""),
      Train(i=5, d=5.1, v=160113, w=100, s="", s2=""),
      Train(i=5, d=5.1, v=160113, w=100, s="", s2=""),
      Train(i=5, d=5.1, v=160113, w=100, s="", s2=""),
      Train(i=5, d=5.1, v=160113, w=106, s="", s2=""),
      Train(i=5, d=5.1, v=160113, w=100, s="", s2=""),
      Train(i=5, d=5.1, v=160113, w=121, s="", s2=""),
      Train(i=5, d=5.1, v=160113, w=100, s="", s2=""),
      Train(i=5, d=5.1, v=160113, w=110, s="", s2=""),
      Train(i=5, d=5.1, v=160113, w=130, s="", s2=""),
      Train(i=5, d=5.1, v=160113, w=110, s="", s2=""),
    ).toDS.toDF

    it("evaluate different clustering models (SSE)"){
      val measure = SSE
      val feat = AssemblyFeature(Seq("i","d","v","w"), "features")

      val allModels = List(
        UnsupervisedModelDesign(
          outputCol="group",
          estimator=Preset.kmeans(features=feat, numK=2, outputCol="group")),
        UnsupervisedModelDesign(
          outputCol="group",
          estimator=Preset.kmeans(features=feat, numK=3, outputCol="group")),
        UnsupervisedModelDesign(
          outputCol="group",
          estimator=Preset.gmm(features=feat, numK=3, outputCol="group", probCol="group_prob"))
      )
      val allScores = new ClusterModelCompare(measure, feat)
        .allOf(dfPreset, allModels)

      allScores.size shouldBe (allModels.size)
      allScores.map{ case(score, m) => m.getClass.getName } shouldBe List(
        "com.tao.thundercats.evaluation.UnsupervisedSpecimen",
        "com.tao.thundercats.evaluation.UnsupervisedSpecimen",
        "com.tao.thundercats.evaluation.UnsupervisedSpecimen")
      allScores.map{ case(score, m) => score } shouldBe List(
        17.142832535885173, 6.4614842171717175, 17.142832535885173)
    }
  }

  describe("Crossvalidation test"){
    lazy val dfPreset = List(
      Train(i=1, d=1.0, v=1.2, w=0.0, s="", s2=""),
      Train(i=2, d=2.0, v=1.5, w=0.0, s="", s2=""),
      Train(i=3, d=3.0, v=2.2, w=0.0, s="", s2=""),
      Train(i=4, d=4.0, v=3.2, w=0.0, s="", s2=""),
      Train(i=5, d=5.0, v=4.2, w=0.0, s="", s2=""),
      Train(i=6, d=6.0, v=5.0, w=0.0, s="", s2=""),
    ).toDS.toDF

    it("Run crossvaliation on N folds"){
      val cv = CrossValidation(
        measure=MPE,
        nFolds=5
      )

      val feature = AssemblyFeature("v"::Nil, "features")
      val design = SupervisedModelDesign(
        outputCol="z",
        labelCol="i",
        estimator=Preset.linearReg(features=feature, labelCol="i", outputCol="z"))
      val score = cv.run(dfPreset, design, feature)

      score.isFailing shouldBe false
      (score.get) should be > 0.0
    }

    it("Run train-test split"){
      val cv = SplitValidation(
        measure=MPE,
        trainRatio=0.65f
      )

      val feature = AssemblyFeature("v"::Nil, "features")
      val design = SupervisedModelDesign(
        outputCol="z",
        labelCol="i",
        estimator=Preset.linearReg(features=feature, labelCol="i", outputCol="z"))
      val score = cv.run(dfPreset, design, feature)

      score.isFailing shouldBe false
      (score.get) should be > 0.0
    }
  }

  describe("Feature selection test"){
    lazy val dfPreset = List(
      Train(i=1, d=1.0, v=1.2, w=0.1, s="1.1", s2=""),
      Train(i=2, d=2.0, v=0.1, w=0.3, s="1.1", s2=""),
      Train(i=3, d=3.2, v=2.2, w=0.5, s="1.3", s2=""),
      Train(i=4, d=4.0, v=3.2, w=0.8, s="0.6", s2=""),
      Train(i=5, d=5.0, v=4.2, w=0.9, s="0.4", s2=""),
      Train(i=6, d=6.1, v=0.0, w=1.1, s="1.9", s2="")
    ).toDS.toDF

    it("Calculate zscores of all features"){
      val select = ZScoreFeatureSelector(AllSignificance)
      val df = dfPreset.withColumn("s", col("s").cast(DoubleType))
      val features = Seq("d", "v", "w", "s")
      val design = SupervisedModelDesign(
        outputCol="z",
        labelCol="i",
        estimator=Preset.linearReg(
          features=AssemblyFeature(features, "features"), 
          labelCol="i",
          outputCol="z"))
      
      val subfeatures = select.selectSubset(
        df, 
        design, 
        features.map(Feature.apply))

      subfeatures.size shouldBe (features.size)
      subfeatures shouldBe (Array(
        (110.87751139732678, Feature("d")),
        (-4.7566538028689775, Feature("v")),
        (6.493176093426949, Feature("w")),
        (-9.049261689285245,Feature("s"))
      ))
    }

    it("Select linear features at confidence level of 90%"){
      val select = ZScoreFeatureSelector(Significance95p)
      val df = dfPreset.withColumn("s", col("s").cast(DoubleType))
      val features = Seq("d", "v", "w", "s")
      val design = SupervisedModelDesign(
        outputCol="z",
        labelCol="i",
        estimator=Preset.linearReg(
          features=AssemblyFeature(features, "features"), 
          labelCol="i",
          outputCol="z"))
      
      val subfeatures = select.selectSubset(
        df, 
        design, 
        features.map(Feature.apply))

      // All selected features should pass minimum z score 
      subfeatures.filter(_._1 >= 1.645) shouldBe subfeatures
      subfeatures.map(_._2.colName) shouldBe Array("d","w")
    }

    it("Select best N linear features"){
      val select = BestNFeaturesSelector(top=2, measure=PearsonCorr)
      val df = dfPreset.withColumn("s", col("s").cast(DoubleType))
      val features = Seq("d", "v", "w", "s")
      val design = SupervisedModelDesign(
        outputCol="z",
        labelCol="i",
        estimator=Preset.linearReg(
          features=AssemblyFeature(features, "features"),
          labelCol="i",
          outputCol="z"))
      
      val subfeatures = select.selectSubset(
        df, 
        design, 
        features.map(Feature.apply))

      subfeatures.size shouldBe 2
    }
  }

  describe("Dimensionality Reduction test"){

    lazy val dfPreset = List(
      Train(i=1, d=1.0, v=1.2, w=0.1, s="1.1", s2=""),
      Train(i=2, d=2.0, v=0.1, w=0.3, s="1.1", s2=""),
      Train(i=3, d=3.2, v=2.2, w=0.5, s="1.3", s2=""),
      Train(i=4, d=4.0, v=3.2, w=0.8, s="0.6", s2=""),
      Train(i=5, d=5.0, v=4.2, w=0.9, s="0.4", s2=""),
      Train(i=6, d=6.1, v=0.0, w=1.1, s="1.9", s2=""),
      Train(i=7, d=7.2, v=5.0, w=1.3, s="0.0", s2=""),
      Train(i=8, d=7.5, v=7.0, w=1.5, s="9.1", s2=""),
      Train(i=9, d=9.4, v=7.7, w=1.8, s="0.0", s2=""),
      Train(i=10, d=9.9, v=8.9, w=2.1, s="0.0", s2="")
    ).toDS.toDF

    it("uses PCA to reduce dimensionality"){
      val df = dfPreset
        .withColumn("s", col("s").cast(DoubleType))
        .withColumn("s2", col("s")*(-1.0))
      val features = AssemblyFeature(Seq("d", "v", "w", "s", "s2"))
      val estimator = Preset.linearReg(
          features=Feature("features"), 
          labelCol="i",
          outputCol="z")

      // Reduce dim: 5 -> 3
      val pipe = features % (
        estimator, 
        preVectorAsmStep=None,
        postVectorAsmStep=Some(DimReduc.PCA(3).asPipelineStage))

      val dfOut = pipe.fit(df).transform(df)

      dfOut.columns should contain ("features")
      dfOut.columns shouldNot contain ("features_reduced")

      val vec = dfOut.rdd.map(_.getAs[DenseVector]("features").toArray).collect
      vec.size shouldBe (df.count)
      forAll (vec) { v => (v.size == 3) shouldBe true }
    }
  }

}
