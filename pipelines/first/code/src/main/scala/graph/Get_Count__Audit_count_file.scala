package graph

import io.prophecy.libs._
import org.apache.spark.sql._
import org.apache.spark.sql.types._

@Visual(id = "Get_Count__Audit_count_file", label = "Get_Count.Audit_count_file", x = 100, y = 100, phase = 3)
object Get_Count__Audit_count_file {

  @UsesDataset(id = "8", version = 0)
  def apply(spark: SparkSession, in: DataFrame): Target = {

    val fabric = "default"
    fabric match {
      case "default" =>
        val schemaArg = StructType(
          Array(
            StructField("program_nm",        StringType,         true),
            StructField("application_nm",    StringType,         true),
            StructField("driver_src_nm",     StringType,         true),
            StructField("ilm_nm",            StringType,         true),
            StructField("run_id",            DecimalType(10, 0), true),
            StructField("run_date",          TimestampType,      true),
            StructField("clean_count",       DecimalType(10, 0), true),
            StructField("test_record_count", DecimalType(10, 0), true),
            StructField("split_count",       DecimalType(10, 0), true)
          )
        )
        in.write.mode(SaveMode.Overwrite).parquet(s"dbfs:/FileStore/optum/Get_Count__Audit_count_file.parquet")
      case _ => throw new Exception("Unknown Fabric")
    }

  }

}
