import org.apache.spark.sql.{Dataset, DataFrame}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Column,Row}
import org.apache.spark.sql.catalyst.encoders._
import org.apache.spark.sql.{Encoders, Encoder}
import org.apache.spark.sql.avro._
import org.apache.spark.sql.functions._

import java.io.File
import sys.process._
import scala.reflect.io.Directory

import com.tao.thundercats.base.{SparkTestInstance, SparkStreamTestInstance}
import com.tao.thundercats.physical._

import org.scalatest._
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
      } yield true

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

    ignore("Filter dataframes"){
      ???
    }

  }

}
