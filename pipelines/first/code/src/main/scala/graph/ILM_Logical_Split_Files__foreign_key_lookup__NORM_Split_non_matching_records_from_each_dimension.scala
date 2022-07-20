package graph

import io.prophecy.libs._
import io.prophecy.libs.SparkFunctions._
import org.apache.spark.sql.ProphecyDataFrame._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

@Visual(id = "ILM_Logical_Split_Files__foreign_key_lookup__NORM_Split_non_matching_records_from_each_dimension",
        label = "ILM_Logical_Split_Files.foreign_key_lookup.NORM_Split_non_matching_records_from_each_dimension",
        x = 100,
        y = 100,
        phase = 2
)
object ILM_Logical_Split_Files__foreign_key_lookup__NORM_Split_non_matching_records_from_each_dimension {

  def apply(spark: SparkSession, in: DataFrame): Normalize = {

    val out_ilm_logical_split_files_foreign_key_lookup_norm_split_non_matching_records_from_each_dimension =
      in.normalize(
        lengthExpression = Some(size(col("dim_match_stat"))),
        finishedExpression = None,
        finishedCondition = None,
        alias = "index",
        colsToSelect = List(
          (element_at(col("dim_match_stat"), col("index") + lit(1)).getField("fsk_hk")).as("fsk_hk"),
          (element_at(col("dim_match_stat"), col("index") + lit(1)).getField("dimension")).as("dimension")
        ),
        tempWindowExpr = Map()
      )

    val simpleSelect_in_DF =
      out_ilm_logical_split_files_foreign_key_lookup_norm_split_non_matching_records_from_each_dimension.select(
        (col("ctrx_d_cag.is_equal").cast(StringType)).as("is_equal"),
        (col("ctrx_d_cag.drop_record").cast(StringType)).as("drop_record"),
        (date_format(to_timestamp(col("ctrx_d_cag.matching__eff_dt").cast(StringType), "yyyy-MM-dd HH:mm:ss"),
                     "yyyy-MM-dd HH:mm:ss"
        )).as("matching__eff_dt"),
        (col("ctrx_d_cag.dxf_src_dataset_id").cast(LongType)).as("dxf_src_dataset_id"),
        (col("ctrx_d_cag.dxf_src_rec_cnt").cast(LongType)).as("dxf_src_rec_cnt"),
        (col("ctrx_d_cag.dxf_src_sys_id").cast(IntegerType)).as("dxf_src_sys_id"),
        (col("ctrx_d_cag.dxf_src_file_name")).as("dxf_src_file_name"),
        (col("fsk_hk")).as("fsk_hk"),
        (array_union(array_union(local_temp7029Udf(), local_temp7335Udf()), local_temp7605Udf())).as("out_fields"),
        (col("dimension")).as("dimension"),
        (col("target")).as("target")
      )

    val out = simpleSelect_in_DF

    out

  }

  def local_temp7605Udf() = {

    val result =
      filter(
        transform(
          string_split_no_empty(
            element_at(
              global_temp6115Udf(),
              (array_position(
                transform(global_temp6115Udf(),
                          x ⇒ struct(x.getField("dim_name").as("dim_name"), x.getField("nk_subrec").as("nk_subrec"))
                ),
                struct(
                  struct(
                    element_at(col("dim_match_stat"), col("index") + lit(1)).getField("dimension").as("dim_name"),
                    element_at(col("dim_match_stat"), col("index") + lit(1)).getField("nk_subrec").as("nk_subrec"),
                    lit("").as("dim_fields_not_in_split_file"),
                    lit("").as("dim_fields_in_split_file"),
                    lit("").as("fsk_field")
                  ).getField("dim_name").as("dim_name"),
                  struct(
                    element_at(col("dim_match_stat"), col("index") + lit(1)).getField("dimension").as("dim_name"),
                    element_at(col("dim_match_stat"), col("index") + lit(1)).getField("nk_subrec").as("nk_subrec"),
                    lit("").as("dim_fields_not_in_split_file"),
                    lit("").as("dim_fields_in_split_file"),
                    lit("").as("fsk_field")
                  ).getField("nk_subrec").as("nk_subrec")
                )
              ) - lit(1)).cast(IntegerType) + lit(1)
            ).getField("fsk_field"),
            lit(";")
          ),
          field ⇒
            struct(when(size(split(field, ".")) === lit(2), element_at(split(field, "."), lit(2)))
                     .otherwise(field)
                     .as("field_name"),
                   lit("0").as("field_value")
            )
        ),
        y ⇒ y.isNotNull
      )

    result
  }

  val ILM_Logical_Split_Files__foreign_key_lookup__NORM_Split_non_matching_records_from_each_dimension_ReinterpretAs_5374_Udf =
    udf(
      (input: Any) =>
        convertInputBytesToStructType(
          input,
          Array("string(\"|\")", "string(\"|\")", "string(\"|\")", "string(\"|\")", "string(\"|\")")
        ),
      StructType(
        List(
          StructField("dim_name",                     StringType, true),
          StructField("nk_subrec",                    StringType, true),
          StructField("dim_fields_not_in_split_file", StringType, true),
          StructField("dim_fields_in_split_file",     StringType, true),
          StructField("fsk_field",                    StringType, true)
        )
      )
    )

  def local_temp7335Udf() = {

    val result =
      filter(
        transform(
          string_split_no_empty(
            element_at(
              global_temp6115Udf(),
              (array_position(
                transform(global_temp6115Udf(),
                          x ⇒ struct(x.getField("dim_name").as("dim_name"), x.getField("nk_subrec").as("nk_subrec"))
                ),
                struct(
                  struct(
                    element_at(col("dim_match_stat"), col("index") + lit(1)).getField("dimension").as("dim_name"),
                    element_at(col("dim_match_stat"), col("index") + lit(1)).getField("nk_subrec").as("nk_subrec"),
                    lit("").as("dim_fields_not_in_split_file"),
                    lit("").as("dim_fields_in_split_file"),
                    lit("").as("fsk_field")
                  ).getField("dim_name").as("dim_name"),
                  struct(
                    element_at(col("dim_match_stat"), col("index") + lit(1)).getField("dimension").as("dim_name"),
                    element_at(col("dim_match_stat"), col("index") + lit(1)).getField("nk_subrec").as("nk_subrec"),
                    lit("").as("dim_fields_not_in_split_file"),
                    lit("").as("dim_fields_in_split_file"),
                    lit("").as("fsk_field")
                  ).getField("nk_subrec").as("nk_subrec")
                )
              ) - lit(1)).cast(IntegerType) + lit(1)
            ).getField("dim_fields_in_split_file"),
            lit(";")
          ),
          field ⇒
            struct(
              when(size(split(field, ".")) === lit(2), element_at(split(field, "."), lit(2)))
                .otherwise(field)
                .as("field_name"),
              when(
                isnull(
                  eval(
                    struct(
                      col("ctrx_d_cag").as("ctrx_d_cag"),
                      col("ctrx_d_carr").as("ctrx_d_carr"),
                      col("c_d_carr_acc").as("c_d_carr_acc"),
                      col("dim_match_stat").as("dim_match_stat"),
                      col("target").as("target"),
                      col("newline").as("newline")
                    ),
                    field
                  ).cast(StringType)
                ),
                lit("")
              ).otherwise(
                  eval(
                    struct(
                      col("ctrx_d_cag").as("ctrx_d_cag"),
                      col("ctrx_d_carr").as("ctrx_d_carr"),
                      col("c_d_carr_acc").as("c_d_carr_acc"),
                      col("dim_match_stat").as("dim_match_stat"),
                      col("target").as("target"),
                      col("newline").as("newline")
                    ),
                    field
                  ).cast(StringType)
                )
                .as("field_value")
            )
        ),
        y ⇒ y.isNotNull
      )

    result
  }

  def local_temp7029Udf() = {

    val result =
      filter(
        transform(
          string_split_no_empty(
            element_at(
              global_temp6115Udf(),
              (array_position(
                transform(global_temp6115Udf(),
                          x ⇒ struct(x.getField("dim_name").as("dim_name"), x.getField("nk_subrec").as("nk_subrec"))
                ),
                struct(
                  struct(
                    element_at(col("dim_match_stat"), col("index") + lit(1)).getField("dimension").as("dim_name"),
                    element_at(col("dim_match_stat"), col("index") + lit(1)).getField("nk_subrec").as("nk_subrec"),
                    lit("").as("dim_fields_not_in_split_file"),
                    lit("").as("dim_fields_in_split_file"),
                    lit("").as("fsk_field")
                  ).getField("dim_name").as("dim_name"),
                  struct(
                    element_at(col("dim_match_stat"), col("index") + lit(1)).getField("dimension").as("dim_name"),
                    element_at(col("dim_match_stat"), col("index") + lit(1)).getField("nk_subrec").as("nk_subrec"),
                    lit("").as("dim_fields_not_in_split_file"),
                    lit("").as("dim_fields_in_split_file"),
                    lit("").as("fsk_field")
                  ).getField("nk_subrec").as("nk_subrec")
                )
              ) - lit(1)).cast(IntegerType) + lit(1)
            ).getField("dim_fields_not_in_split_file"),
            lit(";")
          ),
          field ⇒
            struct(when(size(split(field, ".")) === lit(2), element_at(split(field, "."), lit(2)))
                     .otherwise(field)
                     .as("field_name"),
                   lit("").as("field_value")
            )
        ),
        y ⇒ y.isNotNull
      )

    result
  }

  def global_temp6115Udf() = {

    val result =
      filter(
        transform(
          re_split_no_empty(
            lit("""ids_common.d_carrier|ctrx_d_carr|service_type_nm;super_carrier_id;super_carrier_desc;current_flg;mailg_adr_1;mailg_city;mailg_state;mailg_cntry;mailg_zip_cd;naic_nbr;bob_ind;|ctrx_d_cag.carrier_sk;ctrx_d_cag.dar_lob_sk;ctrx_d_cag.acctg_lob_sk;ctrx_d_carr.carrier_id;ctrx_d_cag.carrier_nm;ctrx_d_cag.carrier_state_cd;ctrx_d_cag.hm_dlvry_only_ind;ctrx_d_cag.hm_dlvry_stat_ind;ctrx_d_cag.rebates_only_ind;ctrx_d_cag.rebates_stat_ind;ctrx_d_cag.src_env_sk;|ctrx_d_cag.carrier_sk;|
ids_common.d_carrier_acct|c_d_carr_acc|c_d_carr_acc.rec_sts;|ctrx_d_cag.carrier_acct_sk;ctrx_d_cag.carrier_sk;c_d_carr_acc.carrier_id;c_d_carr_acc.account_id;ctrx_d_cag.carrier_nm;ctrx_d_cag.account_nm;ctrx_d_cag.acct_client_type_id;ctrx_d_cag.acct_client_type_desc;ctrx_d_cag.acct_product_type_id;ctrx_d_cag.acct_product_type_desc;ctrx_d_cag.acct_product_line_id;ctrx_d_cag.acct_product_line_desc;ctrx_d_cag.hm_dlvry_only_ind;ctrx_d_cag.hm_dlvry_stat_ind;ctrx_d_cag.rebates_only_ind;ctrx_d_cag.rebates_stat_ind;ctrx_d_cag.src_env_sk;c_d_carr_acc.carrier_id;|ctrx_d_cag.carrier_acct_sk;|
"""),
            lit("""[
]+""")
          ),
          line ⇒
            ILM_Logical_Split_Files__foreign_key_lookup__NORM_Split_non_matching_records_from_each_dimension_ReinterpretAs_5374_Udf(
              line
            )
        ),
        y ⇒ y.isNotNull
      )

    result
  }

}
