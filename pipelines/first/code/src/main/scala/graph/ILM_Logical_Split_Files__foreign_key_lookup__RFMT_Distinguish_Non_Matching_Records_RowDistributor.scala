package graph

import io.prophecy.libs.Component._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

@Visual(id = "ILM_Logical_Split_Files__foreign_key_lookup__RFMT_Distinguish_Non_Matching_Records_RowDistributor",
        label = "ILM_Logical_Split_Files.foreign_key_lookup.RFMT_Distinguish_Non_Matching_Records_RowDistributor",
        x = 100,
        y = 100,
        phase = 2
)
object ILM_Logical_Split_Files__foreign_key_lookup__RFMT_Distinguish_Non_Matching_Records_RowDistributor {

  def apply(spark: SparkSession, in: DataFrame): (RowDistributor, RowDistributor) = {

    val out0 = in.filter((size(col("dim_match_stat")) === lit(0)).or(!(size(col("dim_match_stat")) === lit(0))))
    val out1 = in.filter(!(size(col("dim_match_stat")) === lit(0)))

    (out1, out0)

  }

}
