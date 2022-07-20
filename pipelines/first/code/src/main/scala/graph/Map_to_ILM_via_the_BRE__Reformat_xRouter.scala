package graph

import io.prophecy.libs._
import org.apache.spark.sql._

@Visual(id = "Map_to_ILM_via_the_BRE__Reformat_xRouter",
        label = "Map_to_ILM_via_the_BRE.Reformat_xRouter",
        x = 100,
        y = 100,
        phase = 1
)
object Map_to_ILM_via_the_BRE__Reformat_xRouter {

  def apply(spark: SparkSession, in: DataFrame): (RowDistributor, RowDistributor) = {

    val out0 = in
    val out1 = in

    (out1, out0)

  }

}
