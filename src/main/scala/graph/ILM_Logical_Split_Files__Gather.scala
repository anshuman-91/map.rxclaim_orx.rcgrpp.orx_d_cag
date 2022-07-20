package graph

import io.prophecy.libs.Component._
import org.apache.spark.sql._

@Visual(id = "ILM_Logical_Split_Files__Gather", label = "ILM_Logical_Split_Files.Gather", x = 100, y = 100, phase = 2)
object ILM_Logical_Split_Files__Gather {

  def apply(spark: SparkSession, in1: DataFrame): SetOperation = {

    lazy val out = in1

    out

  }

}
