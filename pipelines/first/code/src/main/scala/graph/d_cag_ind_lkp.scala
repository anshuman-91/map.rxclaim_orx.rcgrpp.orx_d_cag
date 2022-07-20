package graph
import io.prophecy.libs._

import io.prophecy.libs._
import org.apache.spark.sql._
import org.apache.spark.sql.types._

@Visual(id = "d_cag_ind_lkp", label = "d_cag_ind_lkp", x = 100, y = 100, phase = -2147483648)
object d_cag_ind_lkp {

  @UsesDataset(id = "4", version = 0)
  def apply(spark: SparkSession): Lookup = {

    val fabric = "default"

    lazy val out = fabric match {
      case "default" =>
        val schemaArg = StructType(
          Array(
            StructField("dxf_sk",            IntegerType,        true),
            StructField("carrier_sk",        DecimalType(10, 0), true),
            StructField("carrier_acct_sk",   DecimalType(10, 0), true),
            StructField("client_id",         StringType,         true),
            StructField("carrier_id",        StringType,         true),
            StructField("account_id",        StringType,         true),
            StructField("employer_group_id", StringType,         true),
            StructField("carrier_nm",        StringType,         true),
            StructField("account_nm",        StringType,         true),
            StructField("employer_group_nm", StringType,         true),
            StructField("contract_num",      StringType,         true),
            StructField("pbp_id",            StringType,         true),
            StructField("gps_plan_cd",       StringType,         true),
            StructField("hm_dlvry_only_ind", StringType,         true),
            StructField("hm_dlvry_stat_ind", StringType,         true),
            StructField("rebates_only_ind",  StringType,         true),
            StructField("rebates_stat_ind",  StringType,         true),
            StructField("ids_updt_dttm",     TimestampType,      true)
          )
        )
        spark.read.parquet("dbfs:/FileStore/tables/lookup_ids_common_d_cag.parquet")
      case _ => throw new Exception(s"The fabric is not handled")
    }
    createLookup(
      "d_cag_ind_lkp",
      out,
      spark,
      List("carrier_id", "account_id", "employer_group_id"),
      "carrier_id",
      "account_id",
      "employer_group_id",
      "hm_dlvry_only_ind",
      "hm_dlvry_stat_ind",
      "rebates_only_ind",
      "rebates_stat_ind"
    )

  }

}
