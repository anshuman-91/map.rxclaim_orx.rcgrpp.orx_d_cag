package graph

import io.prophecy.libs.Component._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

@Visual(id = "ILM_Logical_Split_Files__create_placeholder_files__RowDistributor",
        label = "ILM_Logical_Split_Files.create_placeholder_files.RowDistributor",
        x = 100,
        y = 100,
        phase = 2
)
object ILM_Logical_Split_Files__create_placeholder_files__RowDistributor {

  def apply(spark: SparkSession, in: DataFrame): (RowDistributor, RowDistributor, RowDistributor) = {

    val out0 = in.filter(col("dimension") === lit("ids_common.d_carrier"))
    val out1 = in.filter(col("dimension") === lit("ids_common.d_carrier_acct"))
    val out2 = in
    (out0, out1, out2)

  }

}
