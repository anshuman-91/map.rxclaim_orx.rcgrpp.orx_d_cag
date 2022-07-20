package graph

import io.prophecy.libs.Component._
import io.prophecy.libs.UDFUtils._
import org.apache.spark.sql._
import org.apache.spark.sql.types._

@Visual(id = "cag_gps_plan_code", label = "cag_gps_plan_code", x = 100, y = 100, phase = -2147483648)
object cag_gps_plan_code {

  @UsesDataset(id = "3", version = 0)
  def apply(spark: SparkSession): Lookup = {

    val fabric = "default"

    lazy val out = fabric match {
      case "default" =>
        val schemaArg = StructType(
          Array(
            StructField("dxf_src_dataset_id",     IntegerType, true),
            StructField("dxf_src_rec_cnt",        IntegerType, true),
            StructField("dxf_src_sys_id",         IntegerType, true),
            StructField("dxf_src_file_name",      StringType,  true),
            StructField("source_environment_key", StringType,  true),
            StructField("carrier_id",             StringType,  true),
            StructField("account_id",             StringType,  true),
            StructField("employer_group_id",      StringType,  true),
            StructField("gps_plan_code",          StringType,  true)
          )
        )
        spark.read
          .parquet("dbfs:/FileStore/tables/lookup_rxclaim_cag_gps_plan_code.parquet")
      case _ => throw new Exception(s"The fabric is not handled")
    }
    createLookup(
      "cag_gps_plan_code",
      out,
      spark,
      List("carrier_id", "account_id", "employer_group_id"),
      "dxf_src_dataset_id",
      "dxf_src_rec_cnt",
      "dxf_src_sys_id",
      "dxf_src_file_name",
      "source_environment_key",
      "carrier_id",
      "account_id",
      "employer_group_id",
      "gps_plan_code"
    )

  }

}
