package graph

import io.prophecy.libs._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

@Visual(id = "ILM_Logical_Split_Files__foreign_key_lookup__RFMT_Distinguish_Non_Matching_RecordsReformat_1",
        label = "ILM_Logical_Split_Files.foreign_key_lookup.RFMT_Distinguish_Non_Matching_RecordsReformat_1",
        x = 100,
        y = 100,
        phase = 2
)
object ILM_Logical_Split_Files__foreign_key_lookup__RFMT_Distinguish_Non_Matching_RecordsReformat_1 {

  def apply(spark: SparkSession, in: DataFrame): Reformat = {

    val out = in.select(
      col("ctrx_d_cag").as("ctrx_d_cag"),
      col("ctrx_d_carr").as("ctrx_d_carr"),
      col("c_d_carr_acc").as("c_d_carr_acc"),
      ILM_Logical_Split_Files__foreign_key_lookup__RFMT_Distinguish_Non_Matching_RecordsReformat_1_temp19732Udf()
        .as("dim_match_stat"),
      lit("ids_common.d_cag").as("target"),
      col("newline").as("newline")
    )

    out

  }

  def ILM_Logical_Split_Files__foreign_key_lookup__RFMT_Distinguish_Non_Matching_RecordsReformat_1_temp19732Udf() = {

    val result =
      filter(transform(col("dim_match_stat"),
                       dim_match_entry ⇒ when(dim_match_entry.getField("dim_match_flag") === lit(1), dim_match_entry)
             ),
             y ⇒ y.isNotNull
      )

    result
  }

}
