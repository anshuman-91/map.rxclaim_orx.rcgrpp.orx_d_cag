package graph

import io.prophecy.libs._
import io.prophecy.libs._

import org.apache.spark.sql._
import org.apache.spark.sql.types._

@Visual(id = "bob_180_cag_list_lkp", label = "bob_180_cag_list_lkp", x = 100, y = 100, phase = -2147483648)
object bob_180_cag_list_lkp {

  @UsesDataset(id = "5", version = 0)
  def apply(spark: SparkSession): Lookup = {

    val fabric = "default"

    lazy val out = fabric match {
      case "default" =>
        val schemaArg = StructType(
          Array(
            StructField("dxf_src_sys_id",   IntegerType,        true),
            StructField("carrier_id",       StringType,         true),
            StructField("account_id",       StringType,         true),
            StructField("client_name",      StringType,         true),
            StructField("service_type",     StringType,         true),
            StructField("business_type",    StringType,         true),
            StructField("client_type",      StringType,         true),
            StructField("eff_dt",           DateType,           true),
            StructField("term_dt",          DateType,           true),
            StructField("cag_sk",           DecimalType(10, 0), true),
            StructField("insert_timestamp", TimestampType,      true),
            StructField("newline",          StringType,         true)
          )
        )
        spark.read.parquet("dbfs:/FileStore/tables/lookup_rxclaim_sandbox_bob_180_cag_list.parquet")
      case _ => throw new Exception(s"The fabric is not handled")
    }
    createLookup(
      "bob_180_cag_list_lkp",
      out,
      spark,
      List("carrier_id", "account_id"),
      "dxf_src_sys_id",
      "carrier_id",
      "account_id",
      "client_name",
      "service_type",
      "business_type",
      "client_type",
      "eff_dt",
      "term_dt",
      "cag_sk",
      "insert_timestamp",
      "newline"
    )

  }

}
