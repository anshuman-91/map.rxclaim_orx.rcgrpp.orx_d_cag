package graph

import io.prophecy.libs._
import org.apache.spark.sql._
import org.apache.spark.sql.types._

@Visual(id = "Get_Count__Gather_Logs", label = "Get_Count.Gather_Logs", x = 100, y = 100, phase = 1)
object Get_Count__Gather_Logs {

  @UsesDataset(id = "4", version = 0)
  def apply(spark: SparkSession, in: DataFrame): Target = {

    val fabric = "default"
    fabric match {
      case "default" =>
        val schemaArg = StructType(
          Array(
            StructField("node",         StringType,    true),
            StructField("timestamp",    TimestampType, true),
            StructField("component",    StringType,    true),
            StructField("subcomponent", StringType,    true),
            StructField("event_type",   StringType,    true),
            StructField("event_text",   StringType,    true)
          )
        )
        in.write.mode(SaveMode.Overwrite).parquet(s"dbfs:/FileStore/optum/Get_Count__Gather_Logs.parquet_")
      case _ => throw new Exception("Unknown Fabric")
    }

  }

}
