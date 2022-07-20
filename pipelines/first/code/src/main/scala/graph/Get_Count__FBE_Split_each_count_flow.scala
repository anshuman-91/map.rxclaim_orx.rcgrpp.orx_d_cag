package graph

import io.prophecy.libs._
import io.prophecy.libs.SparkFunctions._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

@Visual(id = "Get_Count__FBE_Split_each_count_flow",
        label = "Get_Count.FBE_Split_each_count_flow",
        x = 100,
        y = 100,
        phase = 1
)
object Get_Count__FBE_Split_each_count_flow {

  def apply(spark: SparkSession, in: DataFrame): (RowDistributor, RowDistributor) = {

    val out0 = in.filter(!string_index(col("component"), lit("Optional_Filter")).cast(BooleanType))
    val out1 = in.filter(string_index(col("component"), lit("Optional_Filter")).cast(BooleanType))

    (out1, out0)

  }

}
