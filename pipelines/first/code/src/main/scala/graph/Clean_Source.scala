package graph

import io.prophecy.libs.Component._
import org.apache.spark.sql._
import org.apache.spark.sql.types._

@Visual(id = "Clean_Source", label = "Clean_Source", x = 100, y = 100, phase = 1)
object Clean_Source {

  @UsesDataset(id = "3", version = 0)
  def apply(spark: SparkSession): Source = {

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
            StructField("acaacd",             StringType,         true),
            StructField("acaccd",             StringType,         true),
            StructField("acadcd",             StringType,         true),
            StructField("acattx",             StringType,         true),
            StructField("acmxtx",             StringType,         true),
            StructField("acmytx",             StringType,         true),
            StructField("acmztx",             StringType,         true),
            StructField("acojst",             StringType,         true),
            StructField("acm0tx",             StringType,         true),
            StructField("achit1",             StringType,         true),
            StructField("achjt1",             StringType,         true),
            StructField("acpxc2",             StringType,         true),
            StructField("aclbnb",             DecimalType(10, 0), true),
            StructField("acm1tx",             StringType,         true),
            StructField("accoda",             DecimalType(10, 0), true),
            StructField("acahdt",             DecimalType(10, 0), true),
            StructField("acmwtx",             StringType,         true),
            StructField("acb4s2",             StringType,         true),
            StructField("acc2dt",             DecimalType(10, 0), true),
            StructField("acadvn",             StringType,         true),
            StructField("acbmdt",             DecimalType(10, 0), true),
            StructField("acabtm",             DecimalType(10, 0), true),
            StructField("acaevn",             StringType,         true),
            StructField("newline",            StringType,         true)
          )
        )
        spark.read.parquet("dbfs:/FileStore/tables/clean_rxclaim_orx_rcgrpp_2020122000001.parquet")
      case _ => throw new Exception(s"The fabric is not handled")
    }

    out

  }

}
