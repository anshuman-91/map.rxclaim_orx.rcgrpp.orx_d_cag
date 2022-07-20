package graph

import io.prophecy.libs._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

@Visual(id = "Get_Count__FBE_Get_finish_event_types",
        label = "Get_Count.FBE_Get_finish_event_types",
        x = 100,
        y = 100,
        phase = 1
)
object Get_Count__FBE_Get_finish_event_types {

  def apply(spark: SparkSession, in: DataFrame): Filter = {

    val out = in.filter(col("event_type") === lit("finish"))

    out

  }

}
