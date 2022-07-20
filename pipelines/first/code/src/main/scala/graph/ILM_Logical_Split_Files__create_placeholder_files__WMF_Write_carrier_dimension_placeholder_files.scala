package graph

import io.prophecy.libs._
import io.prophecy.libs.SparkFunctions._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, LongType, ShortType}

@Visual(id = "ILM_Logical_Split_Files__create_placeholder_files__WMF_Write_carrier_dimension_placeholder_files",
  label = "ILM_Logical_Split_Files.create_placeholder_files.WMF_Write_carrier_dimension_placeholder_files",
  x = 100,
  y = 100,
  phase = 2
)
object ILM_Logical_Split_Files__create_placeholder_files__WMF_Write_carrier_dimension_placeholder_files {

  def apply(spark: SparkSession, in: DataFrame): Target = {

    val out = in.select(
      col("is_equal").cast(DoubleType).as("is_equal"),
      col("drop_record").cast(DoubleType).as("drop_record"),
      to_timestamp(col("matching__eff_dt"), "yyyy-MM-dd HH:mm:ss").as("matching__eff_dt"),
      col("dxf_src_dataset_id").cast(LongType).as("dxf_src_dataset_id"),
      col("dxf_src_rec_cnt").cast(LongType).as("dxf_src_rec_cnt"),
      col("dxf_src_sys_id").cast(ShortType).as("dxf_src_sys_id"),
      col("dxf_src_file_name").as("dxf_src_file_name"),
      col("fsk_hk.dxf_hk_part1").cast(LongType).as("dxf_hk_part1"),
      col("fsk_hk.dxf_hk_part2").cast(LongType).as("dxf_hk_part2"),
      lit(0).cast(LongType).as("dxf_sk"),
      element_at(col("out_fields"), lit(12)).getField("field_value").cast(DoubleType).as("carrier_sk"),
      element_at(col("out_fields"), lit(13)).getField("field_value").cast(DoubleType).as("dar_lob_sk"),
      element_at(col("out_fields"), lit(14)).getField("field_value").cast(DoubleType).as("acctg_lob_sk"),
      replaceBlankColumnWithNull(element_at(col("out_fields"), lit(15)).getField("field_value")).as("carrier_id"),
      replaceBlankColumnWithNull(element_at(col("out_fields"), lit(16)).getField("field_value")).as("carrier_nm"),
      replaceBlankColumnWithNull(element_at(col("out_fields"), lit(1)).getField("field_value")).as("service_type_nm"),
      replaceBlankColumnWithNull(element_at(col("out_fields"), lit(17)).getField("field_value")).as("carrier_state_cd"),
      replaceBlankColumnWithNull(element_at(col("out_fields"), lit(2)).getField("field_value")).as("super_carrier_id"),
      replaceBlankColumnWithNull(element_at(col("out_fields"), lit(3)).getField("field_value")).as("super_carrier_desc"),
      replaceBlankColumnWithNull(element_at(col("out_fields"), lit(4)).getField("field_value")).as("current_flg"),
      replaceBlankColumnWithNull(element_at(col("out_fields"), lit(18)).getField("field_value")).as("hm_dlvry_only_ind"),
      replaceBlankColumnWithNull(element_at(col("out_fields"), lit(19)).getField("field_value")).as("hm_dlvry_stat_ind"),
      replaceBlankColumnWithNull(element_at(col("out_fields"), lit(20)).getField("field_value")).as("rebates_only_ind"),
      replaceBlankColumnWithNull(element_at(col("out_fields"), lit(21)).getField("field_value")).as("rebates_stat_ind"),
      replaceBlankColumnWithNull(element_at(col("out_fields"), lit(22)).getField("field_value")).cast(DoubleType).as("src_env_sk"),
      replaceBlankColumnWithNull(element_at(col("out_fields"), lit(5)).getField("field_value")).as("mailg_adr_1"),
      replaceBlankColumnWithNull(element_at(col("out_fields"), lit(6)).getField("field_value")).as("mailg_city"),
      replaceBlankColumnWithNull(element_at(col("out_fields"), lit(7)).getField("field_value")).as("mailg_state"),
      replaceBlankColumnWithNull(element_at(col("out_fields"), lit(8)).getField("field_value")).as("mailg_cntry"),
      replaceBlankColumnWithNull(element_at(col("out_fields"), lit(9)).getField("field_value")).as("mailg_zip_cd"),
      replaceBlankColumnWithNull(element_at(col("out_fields"), lit(10)).getField("field_value")).as("naic_nbr"),
      replaceBlankColumnWithNull(element_at(col("out_fields"), lit(11)).getField("field_value")).as("bob_ind"),
      lit("\n").as("newline")
    )

    out.write.mode(SaveMode.Overwrite).parquet("dbfs:/FileStore/optum/result/placeholder_orx_d_cag_ids_common_d_cag_ids_common_d_carrier_0.parquet")

  }

}
