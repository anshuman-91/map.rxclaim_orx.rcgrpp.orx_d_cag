package graph

import io.prophecy.libs.Component._
import io.prophecy.libs.UDFUtils._
import org.apache.spark.sql._
import org.apache.spark.sql.types._

@Visual(id = "client_id_cag_mask", label = "client_id_cag_mask", x = 100, y = 100, phase = -2147483648)
object client_id_cag_mask {

  @UsesDataset(id = "6", version = 0)
  def apply(spark: SparkSession): Lookup = {

    val fabric = "default"

    lazy val out = fabric match {
      case "default" =>
        val schemaArg = StructType(
          Array(
            StructField("carrier_id",        StringType, true),
            StructField("account_id",        StringType, true),
            StructField("employer_group_id", StringType, true),
            StructField("client_id",         StringType, true)
          )
        )
        spark.read.parquet("dbfs:/FileStore/tables/lookup_rxclaim_client_id_cag_mask_orx.parquet")
      case _ => throw new Exception(s"The fabric is not handled")
    }
    createLookup("client_id_cag_mask",
                 out,
                 spark,
                 List("carrier_id", "account_id", "employer_group_id"),
                 "carrier_id",
                 "account_id",
                 "employer_group_id",
                 "client_id"
    )

  }

}
