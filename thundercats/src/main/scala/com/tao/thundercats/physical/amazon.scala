package com.tao.thundercats.physical

import org.apache.spark.sql.{Dataset, DataFrame}
import org.apache.spark.sql.SparkSession

import org.apache.hadoop.io.Text
import org.apache.hadoop.dynamodb.DynamoDBItemWritable
import org.apache.hadoop.dynamodb.read.DynamoDBInputFormat
import org.apache.hadoop.dynamodb.write.DynamoDBOutputFormat
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.io.LongWritable

object Amazon {
  object Dynamo {
    def read(region: String, serverAddr: String, tb: String, query: Option[String]=None)
    (implicit spark: SparkSession): DataFrame = {
      // Read DynamoDB table as Hadoop RDD
      val sc = spark.sparkContext
      ???
    } 
  }
}