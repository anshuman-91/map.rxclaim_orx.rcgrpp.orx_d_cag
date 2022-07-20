package graph

import io.prophecy.libs.Component._
import org.apache.spark.sql._

@Visual(id = "Get_Count__Gather_1", label = "Get_Count.Gather_1", x = 100, y = 100, phase = 1)
object Get_Count__Gather_1 {

  def apply(spark: SparkSession, in1: DataFrame): SetOperation = {

    lazy val out = in1

    out

  }

}
