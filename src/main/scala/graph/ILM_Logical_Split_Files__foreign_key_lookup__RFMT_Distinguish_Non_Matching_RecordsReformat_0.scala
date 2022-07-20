package graph

import io.prophecy.libs.Component._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

@Visual(id = "ILM_Logical_Split_Files__foreign_key_lookup__RFMT_Distinguish_Non_Matching_RecordsReformat_0",
        label = "ILM_Logical_Split_Files.foreign_key_lookup.RFMT_Distinguish_Non_Matching_RecordsReformat_0",
        x = 100,
        y = 100,
        phase = 2
)
object ILM_Logical_Split_Files__foreign_key_lookup__RFMT_Distinguish_Non_Matching_RecordsReformat_0 {

  def apply(spark: SparkSession, in: DataFrame): Reformat = {

    val out = in.select(
      col("ctrx_d_cag").as("ctrx_d_cag"),
      col("ctrx_d_carr").as("ctrx_d_carr"),
      col("c_d_carr_acc").as("c_d_carr_acc"),
      col("newline").as("newline")
    )

    out

  }

}
