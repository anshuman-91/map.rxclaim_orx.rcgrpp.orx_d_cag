package graph

import io.prophecy.libs.Component._
import io.prophecy.libs.UDFUtils._
import org.apache.spark.sql._
import org.apache.spark.sql.types._

@Visual(id = "lkp_rcagfp_orx_d_cag_custom",
  label = "lkp_rcagfp_orx_d_cag_custom",
  x = 100,
  y = 100,
  phase = -2147483648
)
object lkp_rcagfp_orx_d_cag_custom {

  @UsesDataset(id = "7", version = 0)
  def apply(spark: SparkSession): Lookup = {

    val fabric = "default"

    lazy val out = fabric match {
      case "default" =>
        val schemaArg = StructType(
          Array(
            StructField("dxf_src_sys_id", IntegerType, true),
            StructField("h8aacd", StringType, true),
            StructField("h8accd", StringType, true),
            StructField("h8adcd", StringType, true),
            StructField("h8gjc6", DecimalType(10, 0), true),
            StructField("h8amt4", StringType, true),
            StructField("tot_str_length", DecimalType(10, 0), true),
            StructField("starting_position", DecimalType(10, 0), true),
            StructField("newline", StringType, true)
          )
        )
        spark.read.parquet("dbfs:/FileStore/tables/lookup_rxclaim_orx_rcagfp_lkp_custom.parquet")
      case _ => throw new Exception(s"The fabric is not handled")
    }
    createLookup(
      "lkp_rcagfp_orx_d_cag_custom",
      out,
      spark,
      List("h8aacd", "h8accd", "h8adcd", "h8amt4", "dxf_src_sys_id"),
      "dxf_src_sys_id",
      "h8aacd",
      "h8accd",
      "h8adcd",
      "h8gjc6",
      "h8amt4",
      "tot_str_length",
      "starting_position",
      "newline"
    )

  }

}
