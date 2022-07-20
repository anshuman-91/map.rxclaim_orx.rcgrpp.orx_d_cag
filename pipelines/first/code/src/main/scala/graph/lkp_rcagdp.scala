package graph

import io.prophecy.libs._
import io.prophecy.libs._
import org.apache.spark.sql._
import org.apache.spark.sql.types._

@Visual(id = "lkp_rcagdp", label = "lkp_rcagdp", x = 100, y = 100, phase = -2147483648)
object lkp_rcagdp {

  @UsesDataset(id = "1", version = 0)
  def apply(spark: SparkSession): Lookup = {

    val fabric = "default"

    lazy val out = fabric match {
      case "default" =>
        val schemaArg = StructType(
          Array(
            StructField("dxf_src_dataset_id", IntegerType,        true),
            StructField("dxf_src_rec_cnt",    IntegerType,        true),
            StructField("dxf_src_sys_id",     IntegerType,        true),
            StructField("dxf_src_file_name",  StringType,         true),
            StructField("cdc_flag",           StringType,         true),
            StructField("cdc_ts",             TimestampType,      true),
            StructField("h7aacd",             StringType,         true),
            StructField("h7accd",             StringType,         true),
            StructField("h7adcd",             StringType,         true),
            StructField("h7dnn3",             DecimalType(10, 0), true),
            StructField("h7khck",             StringType,         true),
            StructField("h7sudt",             DecimalType(10, 0), true),
            StructField("h7svdt",             DecimalType(10, 0), true),
            StructField("h7ant4",             StringType,         true),
            StructField("h7akvn",             StringType,         true),
            StructField("h7c2dt",             DecimalType(10, 0), true),
            StructField("h7adtm",             DecimalType(10, 0), true),
            StructField("h7alvn",             StringType,         true),
            StructField("h7advn",             StringType,         true),
            StructField("h7bmdt",             DecimalType(10, 0), true),
            StructField("h7abtm",             DecimalType(10, 0), true),
            StructField("h7aevn",             StringType,         true),
            StructField("newline",            StringType,         true)
          )
        )
        spark.read
          .parquet("dbfs:/FileStore/tables/lookup_rxclaim_orx_rcagdp_lkp.parquet")
      case _ => throw new Exception(s"The fabric is not handled")
    }
    createLookup(
      "lkp_rcagdp",
      out,
      spark,
      List("h7aacd", "h7accd", "h7adcd", "dxf_src_sys_id"),
      "dxf_src_sys_id",
      "h7aacd",
      "h7accd",
      "h7adcd",
      "h7ant4",
      "newline"
    )

  }

}
