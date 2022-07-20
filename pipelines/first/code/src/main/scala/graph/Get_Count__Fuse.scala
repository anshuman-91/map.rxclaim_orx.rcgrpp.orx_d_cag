package graph

import io.prophecy.libs._
import io.prophecy.libs.SparkFunctions._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

@Visual(id = "Get_Count__Fuse", label = "Get_Count.Fuse", x = 100, y = 100, phase = 3)
object Get_Count__Fuse {

  def apply(spark: SparkSession, right: DataFrame, left: DataFrame): Join = {

    val leftAlias  = left.as("left")
    val rightAlias = right.as("right")
    val dfJoin     = leftAlias.join(rightAlias, left("id") === right("id"), "outer")

    val out = dfJoin.select(
      upper(
        element_at(
          split(lit(
                  "/etl/devel/appconf.rxclaim_idw_direct_v4/idw_direct/users/ssriva40/orca/rxclaim/dml/ilm/ilm.orx_d_cag.dml"
                ),
                "/"
          ),
          size(
            split(lit(
                    "/etl/devel/appconf.rxclaim_idw_direct_v4/idw_direct/users/ssriva40/orca/rxclaim/dml/ilm/ilm.orx_d_cag.dml"
                  ),
                  "/"
            )
          ).cast(IntegerType) - lit(4)
        )
      ).as("program_nm"),
      element_at(
        split(
          lit(
            "/etl/devel/appconf.rxclaim_idw_direct_v4/idw_direct/users/ssriva40/orca/rxclaim/dml/ilm/ilm.orx_d_cag.dml"
          ),
          "/"
        ),
        size(
          split(lit(
                  "/etl/devel/appconf.rxclaim_idw_direct_v4/idw_direct/users/ssriva40/orca/rxclaim/dml/ilm/ilm.orx_d_cag.dml"
                ),
                "/"
          )
        ).cast(IntegerType) - lit(3)
      ).as("application_nm"),
      lit("rxclaim_orx.rcgrpp").as("driver_src_nm"),
      lit("orx_d_cag").as("ilm_nm"),
      lit("0").cast(StringType).as("run_id"),
      date_format(
        to_timestamp(truncateMicroSeconds(lit("YYYYMMddHHmmssSSSSSS"), now()).cast(StringType), "YYYYMMddHHmmss"),
        "yyyyMMddHHmmss"
      ).as("run_date"),
      when(
        (col("left.read_count") - when(isnull(col("right.write_count")), lit(0))
          .otherwise(col("right.write_count"))) === col("left.read_count"),
        col("left.read_count")
      ).otherwise(
          when(col("right.write_count") > col("left.read_count"), col("right.write_count"))
            .otherwise(col("left.read_count"))
        )
        .cast(StringType)
        .as("clean_count"),
      (col("right.read_count") - col("left.read_count")).cast(StringType).as("test_record_count"),
      col("left.write_count").cast(StringType).as("split_count")
    )

    out

  }

}
