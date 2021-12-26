package com.tao.thundercats.physical

import org.apache.spark.sql.{Dataset, DataFrame}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

import org.apache.hadoop.io.Text
import org.apache.hadoop.dynamodb.DynamoDBItemWritable
import org.apache.hadoop.dynamodb.read.DynamoDBInputFormat
import org.apache.hadoop.dynamodb.write.DynamoDBOutputFormat
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.io.LongWritable

object Amazon {

  /**
   * IMPORTANT NOTE:
   * DynamoDB code will fail with file not found issue (known on Github)
   * See: https://github.com/awslabs/emr-dynamodb-connector/issues/110
   */

  object Dynamo {
    def read(region: String, serverAddr: String, tb: String)
    (implicit spark: SparkSession): DataFrame = {
      // Read DynamoDB table as Hadoop RDD
      val sc = spark.sparkContext
      var conf = new JobConf(sc.hadoopConfiguration)
      conf.set("dynamodb.input.tableName", tb)
      conf.set("mapred.output.format.class", "org.apache.hadoop.dynamodb.write.DynamoDBOutputFormat")
      conf.set("mapred.input.format.class", "org.apache.hadoop.dynamodb.read.DynamoDBInputFormat")

      val rddKV = sc.hadoopRDD(
        conf, 
        classOf[DynamoDBInputFormat], 
        classOf[Text], // Key format class
        classOf[DynamoDBItemWritable]) // Value format class

      // Map RDD[(K,V)] => DataFrame
      spark.createDataFrame(rddKV)
    } 
  }
}