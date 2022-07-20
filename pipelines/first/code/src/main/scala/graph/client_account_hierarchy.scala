package graph

import io.prophecy.libs.Component._
import io.prophecy.libs.UDFUtils._
import org.apache.spark.sql._
import org.apache.spark.sql.types._

@Visual(id = "client_account_hierarchy", label = "client_account_hierarchy", x = 100, y = 100, phase = -2147483648)
object client_account_hierarchy {

  @UsesDataset(id = "10", version = 0)
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
            StructField("carrier_id",             StringType,  true),
            StructField("account_id",             StringType,  true),
            StructField("acct_client_type_id",    StringType,  true),
            StructField("acct_client_type_desc",  StringType,  true),
            StructField("acct_product_type_id",   StringType,  true),
            StructField("acct_product_type_desc", StringType,  true),
            StructField("acct_product_line_id",   StringType,  true),
            StructField("acct_product_line_desc", StringType,  true)
          )
        )
        spark.read.parquet("dbfs:/FileStore/tables/lookup_rxclaim_l_client_account_hierarchy.parquet")
      case _ => throw new Exception(s"The fabric is not handled")
    }
    createLookup(
      "client_account_hierarchy",
      out,
      spark,
      List("carrier_id", "account_id"),
      "dxf_src_dataset_id",
      "dxf_src_rec_cnt",
      "dxf_src_sys_id",
      "dxf_src_file_name",
      "carrier_id",
      "account_id",
      "acct_client_type_id",
      "acct_client_type_desc",
      "acct_product_type_id",
      "acct_product_type_desc",
      "acct_product_line_id",
      "acct_product_line_desc"
    )

  }

}
