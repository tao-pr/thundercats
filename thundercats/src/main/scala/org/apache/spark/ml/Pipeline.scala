package org.apache.spark.ml

import java.{util => ju}
import scala.collection.JavaConverters._ // for ju.List.asScala

class CustomPipelineModel(
  override val uid: String, 
  override val stages: Array[Transformer])
extends PipelineModel(uid, stages) {

  def this(uid: String, stages: ju.List[Transformer]) = {
    this(uid, stages.asScala.toArray)
  }

}