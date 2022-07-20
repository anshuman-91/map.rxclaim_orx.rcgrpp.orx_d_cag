package graph

import io.prophecy.libs.Component._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

@Visual(id = "Get_Count__RollUp_Count_in_Serial",
        label = "Get_Count.RollUp_Count_in_Serial",
        x = 100,
        y = 100,
        phase = 1
)
object Get_Count__RollUp_Count_in_Serial {

  def apply(spark: SparkSession, in: DataFrame): Aggregate = {

    val dfGroupBy = in.groupBy(col("component").as("component"))
    val out       = dfGroupBy.agg(sum(col("read_count")).as("read_count"), sum(col("write_count")).as("write_count"))

    out

  }

}
