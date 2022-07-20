package graph

import io.prophecy.libs.Component._
import org.apache.spark.sql._

@Visual(id = "nHandle_Logs", label = "nHandle_Logs", x = 100, y = 100, phase = 1)
object nHandle_Logs {

  def apply(spark: SparkSession, in1: DataFrame): SetOperation = {

    lazy val out = in1

    out

  }

}
