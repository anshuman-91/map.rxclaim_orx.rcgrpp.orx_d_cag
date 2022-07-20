package graph

import io.prophecy.libs.Component._
import io.prophecy.libs.SparkFunctions._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, ShortType}

@Visual(id = "ILM_Logical_Split_Files__create_placeholder_files__WMF_Write_carrier_acct_dimension_placeholder_files",
  label = "ILM_Logical_Split_Files.create_placeholder_files.WMF_Write_carrier_acct_dimension_placeholder_files",
  x = 100,
  y = 100,
  phase = 2
)
object ILM_Logical_Split_Files__create_placeholder_files__WMF_Write_carrier_acct_dimension_placeholder_files {

  def apply(spark: SparkSession, in: DataFrame): Target = {

    val out = in.select(
      struct(
        col("is_equal").cast(DoubleType).as("is_equal"),
        col("drop_record").cast(DoubleType).as("drop_record"),
        to_timestamp(col("matching__eff_dt"), "yyyy-MM-dd HH:mm:ss").as("matching__eff_dt"),
        col("dxf_src_dataset_id").cast(LongType).as("dxf_src_dataset_id"),
        col("dxf_src_rec_cnt").cast(IntegerType).as("dxf_src_rec_cnt"),
        lit(0).cast(ShortType).as("dxf_src_sys_id"),
        col("dxf_src_file_name").as("dxf_src_file_name"),
        col("fsk_hk.dxf_hk_part1").cast(LongType).as("dxf_hk_part1"),
        col("fsk_hk.dxf_hk_part2").cast(LongType).as("dxf_hk_part2"),
        callUDF("Regular_HK_UK_Lookup_Template_Acct", col("fsk_hk")).getField("dxf_sk").as("dxf_sk"),
        element_at(col("out_fields"), lit(2)).getField("field_value").cast(DoubleType).as("carrier_acct_sk"),
        element_at(col("out_fields"), lit(3)).getField("field_value").cast(DoubleType).as("carrier_sk"),
        replaceBlankColumnWithNull(element_at(col("out_fields"), lit(4)).getField("field_value")).as("carrier_id"),
        replaceBlankColumnWithNull(element_at(col("out_fields"), lit(5)).getField("field_value")).as("account_id"),
        replaceBlankColumnWithNull(element_at(col("out_fields"), lit(6)).getField("field_value")).as("carrier_nm"),
        replaceBlankColumnWithNull(element_at(col("out_fields"), lit(7)).getField("field_value")).as("account_nm"),
        replaceBlankColumnWithNull(element_at(col("out_fields"), lit(8)).getField("field_value")).as("acct_client_type_id"),
        replaceBlankColumnWithNull(element_at(col("out_fields"), lit(9)).getField("field_value")).as("acct_client_type_desc"),
        replaceBlankColumnWithNull(element_at(col("out_fields"), lit(10)).getField("field_value")).as("acct_product_type_id"),
        replaceBlankColumnWithNull(element_at(col("out_fields"), lit(11)).getField("field_value")).as("acct_product_type_desc"),
        replaceBlankColumnWithNull(element_at(col("out_fields"), lit(12)).getField("field_value")).as("acct_product_line_id"),
        replaceBlankColumnWithNull(element_at(col("out_fields"), lit(13)).getField("field_value")).as("acct_product_line_desc"),
        replaceBlankColumnWithNull(element_at(col("out_fields"), lit(1)).getField("field_value")).as("rec_sts"),
        element_at(col("out_fields"), lit(14)).getField("field_value").as("hm_dlvry_only_ind"),
        element_at(col("out_fields"), lit(15)).getField("field_value").as("hm_dlvry_stat_ind"),
        element_at(col("out_fields"), lit(16)).getField("field_value").as("rebates_only_ind"),
        element_at(col("out_fields"), lit(17)).getField("field_value").as("rebates_stat_ind"),
        element_at(col("out_fields"), lit(18)).getField("field_value").cast(DoubleType).as("src_env_sk"),
      ).as("c_d_carr_acc"),
      struct(
        replaceBlankColumnWithNull(element_at(col("out_fields"), lit(14)).getField("field_value")).as("carrier_id")
      ).as("d_carr"),
      lit("\n").as("newline")
    )

    out.write.mode(SaveMode.Overwrite).parquet("dbfs:/FileStore/optum/result/placeholder_orx_d_cag_ids_common_d_cag_ids_common_d_carrier_acct_0.parquet")

  }

}
