package graph

import io.prophecy.libs._
import io.prophecy.libs._
import org.apache.spark.sql._
import org.apache.spark.sql.types._

@Visual(id = "rccagp", label = "rccagp", x = 100, y = 100, phase = -2147483648)
object rccagp {

  @UsesDataset(id = "9", version = 0)
  def apply(spark: SparkSession): Lookup = {

    val fabric = "default"

    lazy val out = fabric match {
      case "default" =>
        val schemaArg = StructType(
          Array(
            StructField("dxf_src_dataset_id", IntegerType,   true),
            StructField("dxf_src_rec_cnt",    IntegerType,   true),
            StructField("dxf_src_sys_id",     IntegerType,   true),
            StructField("dxf_src_file_name",  StringType,    true),
            StructField("cdc_flag",           StringType,    true),
            StructField("cdc_ts",             TimestampType, true),
            StructField("hyfqc6",             StringType,    true),
            StructField("hyfrc6",             StringType,    true),
            StructField("hyfsc6",             StringType,    true),
            StructField("hyf4c6",             StringType,    true),
            StructField("newline",            StringType,    true)
          )
        )
        spark.read.parquet("dbfs:/FileStore/tables/lookup_rxclaim_orx_rccagp.parquet")
      case _ => throw new Exception(s"The fabric is not handled")
    }
    createLookup(
      "rccagp",
      out,
      spark,
      List("hyfqc6", "hyfrc6", "hyfsc6"),
      "dxf_src_dataset_id",
      "dxf_src_rec_cnt",
      "dxf_src_sys_id",
      "dxf_src_file_name",
      "cdc_flag",
      "cdc_ts",
      "hyfqc6",
      "hyfrc6",
      "hyfsc6",
      "hyf4c6",
      "newline"
    )

  }

}
