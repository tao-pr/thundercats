import org.apache.spark.sql.{Dataset, DataFrame}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Column,Row}
import org.apache.spark.sql.catalyst.encoders._
import org.apache.spark.sql.{Encoders, Encoder}
import org.apache.spark.sql.avro._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import java.io.File
import sys.process._
import scala.reflect.io.Directory

import com.tao.thundercats.base.{SparkTestInstance, SparkStreamTestInstance}
import com.tao.thundercats.physical._
import com.tao.thundercats.functional._

import org.scalatest.{Filter => _, _}
import Matchers._

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

class DataSuite extends FunSpec with Matchers with SparkStreamTestInstance {

  describe("Basic IO"){
    import spark.implicits._
    lazy val tempCSV = File.createTempFile("tc-test-", ".csv").getAbsolutePath
    lazy val tempParquet = File.createTempFile("tc-test-", ".parquet").getAbsolutePath

    val topic = "tc-test"
    val topic2 = "tck-test"
    val topic3 = "tckk-test"
    val serverAddr = "localhost"

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
        val cmd = s"kafka-topics --bootstrap-server ${serverAddr}:9092 --topic ${top} --delete"
        Console.println(Console.YELLOW + s"Executing ${cmd}" + Console.RESET)
        cmd !
      }
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

      dfReadOpt.getError.map(_.contains("Path does not exist: file:/Users/pataoengineer/code/thundercats/not-found.csv;")).getOrElse("") shouldBe true
      dfReadOpt.isFailing shouldBe true
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
        b <- Write.kafka(dfK, topic, serverAddr)
        c <- Read.kafka(topic, serverAddr)
      } yield c

      val dfRead = dfReadOpt.get

      dfRead.count shouldBe (dfK.count)
      dfRead.map(_.getAs[String]("key")).collect shouldBe (Seq("foo1", "foo2", "foo3"))
    }

    it("write and read Kafka (stream)"){
      // Fill kafka topic and read by stream
      for {
        _ <- Write.kafka(dfK, topic2, serverAddr)
        b <- Read.kafkaStream(topic2, serverAddr)
        _ <- Screen.showDFStream(b, Some("Initial stream messages"))
      } yield b

      // Read from one topic and write to another
      val dff = for {
        b <- Read.kafkaStream(topic2, serverAddr)
        _ <- Write.kafkaStream(b, topic3, serverAddr, timeout=Some(1000), checkpointLocation="./chk3")
        c <- Read.kafka(topic3, serverAddr)
      } yield c

      dff.get.count shouldBe (dfK.count)
      dff.get.map(_.getAs[String]("key")).collect shouldBe (Seq("foo1", "foo2", "foo3"))
    }

    it("write stream to parquet and csv"){
      // Read from Kafka stream and stream to parquet file
      val dfOpt = for {
        b <- Read.kafkaStream(topic2, serverAddr)
        _ <- Write.streamToFile(b, "parquet", "./out_parquet", checkpointLocation="./chk_parq", timeout=Some(1000))
        _ <- Screen.showDFStream(b, Some("Streaming this into parquet"))
        c <- Read.parquet("./out_parquet")
      } yield c

      dfOpt.get.count shouldBe (dfK.count)
      dfOpt.get.map(_.getAs[String]("key")).collect shouldBe (Seq("foo1", "foo2", "foo3"))
    }

    it("POST: cleanup tempfiles"){
      IO.deleteFiles(tempCSV :: tempParquet :: Nil)
    }

    it("POST: flush Kafka"){
      Seq(topic, topic2, topic3).foreach{ top => 
        s"kafka-topics --bootstrap-server ${serverAddr}:9092 --topic ${top} --delete" !
      }
    }

    it("POST: cleanup checkpoint directories"){
      IO.cleanupCheckpointDirs()
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
        a <- F.addColumn(dfK1, "b", when('v1==="222", lit(null)).otherwise(sequence(lit(0), lit(5), lit(1))))
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

    import spark.implicits._
    import Implicits._

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

  describe("Optimisation test"){

    import spark.implicits._
    import Implicits._

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

  describe("Feature engineering test"){

    import spark.implicits._
    import Implicits._

    
  }

  describe("Modeling test"){

    import spark.implicits._
    import Implicits._

  }

}
