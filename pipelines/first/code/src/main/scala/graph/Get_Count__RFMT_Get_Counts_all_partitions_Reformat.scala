package graph

import io.prophecy.libs.Component._
import io.prophecy.libs.SparkFunctions._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

@Visual(id = "Get_Count__RFMT_Get_Counts_all_partitions_Reformat",
        label = "Get_Count.RFMT_Get_Counts_all_partitions_Reformat",
        x = 100,
        y = 100,
        phase = 1
)
object Get_Count__RFMT_Get_Counts_all_partitions_Reformat {

  def apply(spark: SparkSession, in: DataFrame): Reformat = {

    val out = in.select(
      when(string_index(col("component"), lit("Map_to_ILM")).cast(BooleanType),
           element_at(split(col("component"), "\\."), lit(1))
      ).otherwise(
          concat(element_at(split(col("component"), "\\."), lit(1)),
                 lit("."),
                 element_at(split(col("component"), "\\."), lit(2))
          )
        )
        .as("component"),
      string_filter(element_at(split(col("event_text"), "\\("), lit(1)), lit("0123456789"))
        .cast(StringType)
        .as("read_count"),
      string_filter(element_at(split(element_at(split(col("event_text"), "\\("), lit(2)), ")"), lit(2)),
                    lit("0123456789")
      ).cast(StringType).as("write_count")
    )

    out

  }

}
