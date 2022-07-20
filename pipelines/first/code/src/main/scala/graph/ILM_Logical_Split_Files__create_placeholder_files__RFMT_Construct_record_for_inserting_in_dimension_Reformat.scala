package graph

import io.prophecy.libs._
import io.prophecy.libs.SparkFunctions._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

@Visual(
  id = "ILM_Logical_Split_Files__create_placeholder_files__RFMT_Construct_record_for_inserting_in_dimension_Reformat",
  label = "ILM_Logical_Split_Files.create_placeholder_files.RFMT_Construct_record_for_inserting_in_dimension_Reformat",
  x = 100,
  y = 100,
  phase = 2
)
object ILM_Logical_Split_Files__create_placeholder_files__RFMT_Construct_record_for_inserting_in_dimension_Reformat {

  def apply(spark: SparkSession, in: DataFrame): Reformat = {

    val out = in.select(
      col("dimension").as("dimension"),
      col("target").as("target"),
      col("is_equal").as("is_equal"),
      col("drop_record").as("drop_record"),
      col("matching__eff_dt").as("matching__eff_dt"),
      col("dxf_src_dataset_id").cast(LongType).as("dxf_src_dataset_id"),
      col("dxf_src_rec_cnt").cast(LongType).as("dxf_src_rec_cnt"),
      col("dxf_src_sys_id").cast(IntegerType).as("dxf_src_sys_id"),
      col("dxf_src_file_name").as("dxf_src_file_name"),
      col("fsk_hk.dxf_hk_part1").cast(LongType).as("dxf_hk_part1"),
      col("fsk_hk.dxf_hk_part2").cast(LongType).as("dxf_hk_part2"),
      lit(0).cast(LongType).as("dxf_sk"),
      ILM_Logical_Split_Files__create_placeholder_files__RFMT_Construct_record_for_inserting_in_dimension_Reformat_record_to_addUdf()
        .as("target_fields_line")
    )

    out

  }

  def local_temp2108Udf() = {

    val result =
      filter(
        transform(
          string_split_no_empty(
            lit(
              "ids_common.d_carrier|is_equal;drop_record;matching__eff_dt;dxf_src_dataset_id;dxf_src_rec_cnt;dxf_src_sys_id;dxf_src_file_name;dxf_hk_part1;dxf_hk_part2;dxf_sk;carrier_sk;dar_lob_sk;acctg_lob_sk;carrier_id;carrier_nm;service_type_nm;carrier_state_cd;super_carrier_id;super_carrier_desc;current_flg;hm_dlvry_only_ind;hm_dlvry_stat_ind;rebates_only_ind;rebates_stat_ind;src_env_sk;mailg_adr_1;mailg_city;mailg_state;mailg_cntry;mailg_zip_cd;naic_nbr;bob_ind;newline|\nids_common.d_carrier_acct|is_equal;drop_record;matching__eff_dt;dxf_src_dataset_id;dxf_src_rec_cnt;dxf_src_sys_id;dxf_src_file_name;dxf_hk_part1;dxf_hk_part2;dxf_sk;carrier_acct_sk;carrier_sk;carrier_id;account_id;carrier_nm;account_nm;acct_client_type_id;acct_client_type_desc;acct_product_type_id;acct_product_type_desc;acct_product_line_id;acct_product_line_desc;rec_sts;hm_dlvry_only_ind;hm_dlvry_stat_ind;rebates_only_ind;rebates_stat_ind;src_env_sk;carrier_id;newline|"
            ),
            lit("\n")
          ),
          line ⇒
            ILM_Logical_Split_Files__create_placeholder_files__RFMT_Construct_record_for_inserting_in_dimension_Reformat_ReinterpretAs_1802_Udf(
              line
            )
        ),
        y ⇒ y.isNotNull
      )

    result
  }

  def ILM_Logical_Split_Files__create_placeholder_files__RFMT_Construct_record_for_inserting_in_dimension_Reformat_record_to_addUdf() = {
    val record_to_add = lit("")
    val result =
      filter(
        transform(
          split(
            element_at(
              local_temp2108Udf(),
              (array_position(
                transform(local_temp2108Udf(), x ⇒ struct(x.getField("target").as("target"))),
                struct(
                  struct(col("dimension").as("target"),
                         lit("").as("target_field_names"),
                         lit("").as("target_field_typenames")
                  ).getField("target").as("target")
                )
              ) - lit(1)).cast(IntegerType) + lit(1)
            ).getField("target_field_names"),
            ";"
          ),
          field ⇒
            when(
              (!array_contains(
                string_split_no_empty(
                  lit(
                    "is_equal|drop_record|matching__eff_dt|dxf_src_dataset_id|dxf_src_rec_cnt|dxf_src_sys_id|dxf_src_file_name|dxf_hk_part1|dxf_hk_part2|dxf_sk|newline"
                  ),
                  lit("|")
                ),
                field
              )).and(
                (array_position(
                  transform(col("out_fields"), x ⇒ struct(x.getField("field_name").as("field_name"))),
                  struct(
                    struct(field.as("field_name"), lit("").as("field_value")).getField("field_name").as("field_name")
                  )
                ) - lit(1)) =!= lit(-1)
              ),
              concat(
                element_at(
                  col("out_fields"),
                  (array_position(
                    transform(col("out_fields"), x ⇒ struct(x.getField("field_name").as("field_name"))),
                    struct(
                      struct(field.as("field_name"), lit("").as("field_value")).getField("field_name").as("field_name")
                    )
                  ) - lit(1)).cast(IntegerType) + lit(1)
                ).getField("field_value"),
                lit("\\x01")
              )
            )
        ),
        y ⇒ y.isNotNull
      )

    aggregate(result, record_to_add, (acc, x) ⇒ concat(acc, x))

  }

  val ILM_Logical_Split_Files__create_placeholder_files__RFMT_Construct_record_for_inserting_in_dimension_Reformat_ReinterpretAs_1802_Udf =
    udf(
      (input: Any) => convertInputBytesToStructType(input, Array("string(\"|\")", "string(\"|\")")),
      StructType(
        List(
          StructField("target",             StringType, true),
          StructField("target_field_names", StringType, true)
        )
      )
    )

}
