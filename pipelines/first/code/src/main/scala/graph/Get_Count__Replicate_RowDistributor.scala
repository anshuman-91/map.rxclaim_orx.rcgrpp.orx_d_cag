package graph

import io.prophecy.libs.Component._
import org.apache.spark.sql._

@Visual(id = "Get_Count__Replicate_RowDistributor",
        label = "Get_Count.Replicate_RowDistributor",
        x = 100,
        y = 100,
        phase = 1
)
object Get_Count__Replicate_RowDistributor {

  def apply(spark: SparkSession, in: DataFrame): (RowDistributor, RowDistributor) = {

    val out0 = in
    val out1 = in

    (out1, out0)

  }

}
