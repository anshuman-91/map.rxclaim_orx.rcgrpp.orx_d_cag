package graph

import io.prophecy.libs.Component._
import io.prophecy.libs.UDFUtils._
import org.apache.spark.sql._
import org.apache.spark.sql.types._

@Visual(id = "d_carrier_acct", label = "d_carrier_acct", x = 100, y = 100, phase = -2147483648)
object d_carrier_acct {

  @UsesDataset(id = "2", version = 0)
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
            StructField("carrier_id",         StringType,         true),
            StructField("account_id",         StringType,         true),
            StructField("carrier_nm",         StringType,         true),
            StructField("account_nm",         StringType,         true),
            StructField("dxf_hk_part1",       IntegerType,        true),
            StructField("dxf_hk_part2",       IntegerType,        true),
            StructField("dxf_sk",             IntegerType,        true),
            StructField("src_env_sk",         DecimalType(10, 0), true),
            StructField("ids_updt_dttm",      TimestampType,      true)
          )
        )
        spark.read.parquet("dbfs:/FileStore/tables/lookup_ids_common_d_carrier_acct.parquet")
      case _ => throw new Exception(s"The fabric is not handled")
    }
    createLookup(
      "d_carrier_acct",
      out,
      spark,
      List("carrier_id", "account_id"),
      "dxf_src_dataset_id",
      "dxf_src_rec_cnt",
      "dxf_src_sys_id",
      "dxf_src_file_name",
      "carrier_id",
      "account_id",
      "carrier_nm",
      "account_nm",
      "dxf_hk_part1",
      "dxf_hk_part2",
      "dxf_sk",
      "src_env_sk",
      "ids_updt_dttm"
    )

  }

}
