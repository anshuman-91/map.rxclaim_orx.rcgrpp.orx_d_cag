package graph

import io.prophecy.libs.Component._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

@Visual(id = "ILM_Logical_Split_Files__foreign_key_lookup__RFMT_Split_ICFF_and_Regular_Lookup_records_Reformat",
        label = "ILM_Logical_Split_Files.foreign_key_lookup.RFMT_Split_ICFF_and_Regular_Lookup_records_Reformat",
        x = 100,
        y = 100,
        phase = 1
)
object ILM_Logical_Split_Files__foreign_key_lookup__RFMT_Split_ICFF_and_Regular_Lookup_records_Reformat {

  def apply(spark: SparkSession, in: DataFrame): Reformat = {

    val out = in.select(
      col("ctrx_d_cag").as("ctrx_d_cag"),
      col("ctrx_d_carr").as("ctrx_d_carr"),
      col("c_d_carr_acc").as("c_d_carr_acc"),
      array().as("dim_match_stat"),
      spark_partition_id().cast(StringType).as("partition_nbr"),
      monotonically_increasing_id().cast(StringType).as("record_nbr"),
      col("newline").as("newline")
    )

    out

  }

}
