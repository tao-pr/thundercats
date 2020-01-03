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
}

class DataSuite extends FunSpec with Matchers with SparkStreamTestInstance {

  describe("Basic IO"){
    lazy val tempCSV = File.createTempFile("tc-test-", ".csv").getAbsolutePath

    it("PRE: cleanup tempfiles"){
      IO.deleteFiles(tempCSV :: Nil)
    }

    it("POST: cleanup tempfiles"){
      IO.deleteFiles(tempCSV :: Nil)
    }
  }

}
