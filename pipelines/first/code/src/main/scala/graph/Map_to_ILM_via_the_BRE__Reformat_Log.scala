package graph

import io.prophecy.libs.Component._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

@Visual(id = "Map_to_ILM_via_the_BRE__Reformat_Log",
        label = "Map_to_ILM_via_the_BRE.Reformat_Log",
        x = 100,
        y = 100,
        phase = 1
)
object Map_to_ILM_via_the_BRE__Reformat_Log {

  def apply(spark: SparkSession, in: DataFrame): Select = {

    lazy val out = in.select(
      lit("Map_to_ILM_via_the_BRE__Reformat").as("component"),
      lit("finish").as("event_type"),
      lit("").as("event_text"),
      count(lit(1)).as("no_of_records_read"),
      count(lit(1)).as("no_of_records_written"),
      lit(0).as("no_of_records_rejected")
    )

    out

  }

}
