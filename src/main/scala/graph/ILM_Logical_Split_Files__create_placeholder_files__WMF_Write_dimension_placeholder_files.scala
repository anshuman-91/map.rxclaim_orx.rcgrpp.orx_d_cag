package graph

import io.prophecy.libs.Component._
import org.apache.spark.sql.ProphecyDataFrame._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

@Visual(id = "ILM_Logical_Split_Files__create_placeholder_files__WMF_Write_dimension_placeholder_files",
        label = "ILM_Logical_Split_Files.create_placeholder_files.WMF_Write_dimension_placeholder_files",
        x = 100,
        y = 100,
        phase = 2
)
object ILM_Logical_Split_Files__create_placeholder_files__WMF_Write_dimension_placeholder_files {

  def apply(spark: SparkSession, in: DataFrame): MultiFileWrite = {

    val withFileDF = in.withColumn(
      "fileName",
      concat(
        concat(
          concat(concat(concat(lit("dbfs:/FileStore/optum/result/placeholder."),
                               lit("orx_d_cag.")
                        ),
                        col("target")
                 ),
                 lit(".")
          ),
          col("dimension")
        ),
        lit(".0.parquet")
      )
    )
    withFileDF.breakAndWriteDataFrameForOutputFile(
      List(
        "is_equal",
        "drop_record",
        "matching__eff_dt",
        "dxf_src_dataset_id",
        "dxf_src_rec_cnt",
        "dxf_src_sys_id",
        "dxf_src_file_name",
        "dxf_hk_part1",
        "dxf_hk_part2",
        "dxf_sk",
        "target_fields_line"
      ),
      "fileName",
      "parquet"
    )

  }

}
