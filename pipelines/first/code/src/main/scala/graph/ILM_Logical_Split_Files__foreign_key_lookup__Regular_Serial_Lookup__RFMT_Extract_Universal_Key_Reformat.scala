package graph

import io.prophecy.libs.Component._
import io.prophecy.libs.SparkFunctions._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

@Visual(id = "ILM_Logical_Split_Files__foreign_key_lookup__Regular_Serial_Lookup__RFMT_Extract_Universal_Key_Reformat",
        label = "ILM_Logical_Split_Files.foreign_key_lookup.Regular_Serial_Lookup.RFMT_Extract_Universal_Key_Reformat",
        x = 100,
        y = 100,
        phase = 2
)
object ILM_Logical_Split_Files__foreign_key_lookup__Regular_Serial_Lookup__RFMT_Extract_Universal_Key_Reformat {

  def apply(spark: SparkSession, in: DataFrame): Reformat = {

    val out = in.select(
      struct(
        col("ctrx_d_cag.is_equal").as("is_equal"),
        col("ctrx_d_cag.drop_record").as("drop_record"),
        col("ctrx_d_cag.matching__eff_dt").as("matching__eff_dt"),
        col("ctrx_d_cag.dxf_src_dataset_id").cast(LongType).as("dxf_src_dataset_id"),
        col("ctrx_d_cag.dxf_src_rec_cnt").cast(LongType).as("dxf_src_rec_cnt"),
        col("ctrx_d_cag.dxf_src_sys_id").cast(IntegerType).as("dxf_src_sys_id"),
        col("ctrx_d_cag.dxf_src_file_name").as("dxf_src_file_name"),
        col("ctrx_d_cag.dxf_hk_part1").cast(LongType).as("dxf_hk_part1"),
        col("ctrx_d_cag.dxf_hk_part2").cast(LongType).as("dxf_hk_part2"),
        col("ctrx_d_cag.dxf_sk").cast(LongType).as("dxf_sk"),
        col("ctrx_d_cag.cag_sk").cast(DecimalType(38,       0)).cast(StringType).as("cag_sk"),
        col("ctrx_d_cag.dar_lob_sk").cast(DecimalType(38,   0)).cast(StringType).as("dar_lob_sk"),
        col("ctrx_d_cag.acctg_lob_sk").cast(DecimalType(38, 0)).cast(StringType).as("acctg_lob_sk"),
        col("ctrx_d_cag.client_sk").cast(DecimalType(38,    0)).cast(StringType).as("client_sk"),
        when(
          isnull(
            callUDF(
              "Regular_HK_UK_Lookup_Template",
              ILM_Logical_Split_Files__foreign_key_lookup__Regular_Serial_Lookup__RFMT_Extract_Universal_Key_Reformat_ReinterpretAs_7235_Udf(
                murmur(
                  struct(
                    struct(
                      col("ctrx_d_cag").as("ctrx_d_cag"),
                      col("ctrx_d_carr").as("ctrx_d_carr"),
                      col("c_d_carr_acc").as("c_d_carr_acc"),
                      col("dim_match_stat").as("dim_match_stat"),
                      col("partition_nbr").as("partition_nbr"),
                      col("record_nbr").as("record_nbr"),
                      col("newline").as("newline")
                    ).getField("ctrx_d_carr").as("ctrx_d_carr")
                  )
                )
              )
            ).getField("dxf_sk")
          ),
          lit(0)
        ).otherwise(
            callUDF(
              "Regular_HK_UK_Lookup_Template",
              ILM_Logical_Split_Files__foreign_key_lookup__Regular_Serial_Lookup__RFMT_Extract_Universal_Key_Reformat_ReinterpretAs_7235_Udf(
                murmur(
                  struct(
                    struct(
                      col("ctrx_d_cag").as("ctrx_d_cag"),
                      col("ctrx_d_carr").as("ctrx_d_carr"),
                      col("c_d_carr_acc").as("c_d_carr_acc"),
                      col("dim_match_stat").as("dim_match_stat"),
                      col("partition_nbr").as("partition_nbr"),
                      col("record_nbr").as("record_nbr"),
                      col("newline").as("newline")
                    ).getField("ctrx_d_carr").as("ctrx_d_carr")
                  )
                )
              )
            ).getField("dxf_sk")
          )
          .cast(StringType)
          .as("carrier_sk"),
        when(
          (lit("ids_common.d_carrier_acct") === lit("ids_common.d_carrier")).and(
            ILM_Logical_Split_Files__foreign_key_lookup__Regular_Serial_Lookup__RFMT_Extract_Universal_Key_Reformat_ReinterpretAs_7235_Udf(
              murmur(
                struct(
                  struct(
                    col("ctrx_d_cag").as("ctrx_d_cag"),
                    col("ctrx_d_carr").as("ctrx_d_carr"),
                    col("c_d_carr_acc").as("c_d_carr_acc"),
                    col("dim_match_stat").as("dim_match_stat"),
                    col("partition_nbr").as("partition_nbr"),
                    col("record_nbr").as("record_nbr"),
                    col("newline").as("newline")
                  ).getField("c_d_carr_acc").as("c_d_carr_acc")
                )
              )
            ) === ILM_Logical_Split_Files__foreign_key_lookup__Regular_Serial_Lookup__RFMT_Extract_Universal_Key_Reformat_ReinterpretAs_7235_Udf(
              murmur(
                struct(
                  struct(
                    col("ctrx_d_cag").as("ctrx_d_cag"),
                    col("ctrx_d_carr").as("ctrx_d_carr"),
                    col("c_d_carr_acc").as("c_d_carr_acc"),
                    col("dim_match_stat").as("dim_match_stat"),
                    col("partition_nbr").as("partition_nbr"),
                    col("record_nbr").as("record_nbr"),
                    col("newline").as("newline")
                  ).getField("ctrx_d_carr").as("ctrx_d_carr")
                )
              )
            )
          ),
          when(
            isnull(
              callUDF(
                "Regular_HK_UK_Lookup_Template",
                ILM_Logical_Split_Files__foreign_key_lookup__Regular_Serial_Lookup__RFMT_Extract_Universal_Key_Reformat_ReinterpretAs_7235_Udf(
                  murmur(
                    struct(
                      struct(
                        col("ctrx_d_cag").as("ctrx_d_cag"),
                        col("ctrx_d_carr").as("ctrx_d_carr"),
                        col("c_d_carr_acc").as("c_d_carr_acc"),
                        col("dim_match_stat").as("dim_match_stat"),
                        col("partition_nbr").as("partition_nbr"),
                        col("record_nbr").as("record_nbr"),
                        col("newline").as("newline")
                      ).getField("ctrx_d_carr").as("ctrx_d_carr")
                    )
                  )
                )
              ).getField("dxf_sk")
            ),
            lit(0)
          ).otherwise(
              callUDF(
                "Regular_HK_UK_Lookup_Template",
                ILM_Logical_Split_Files__foreign_key_lookup__Regular_Serial_Lookup__RFMT_Extract_Universal_Key_Reformat_ReinterpretAs_7235_Udf(
                  murmur(
                    struct(
                      struct(
                        col("ctrx_d_cag").as("ctrx_d_cag"),
                        col("ctrx_d_carr").as("ctrx_d_carr"),
                        col("c_d_carr_acc").as("c_d_carr_acc"),
                        col("dim_match_stat").as("dim_match_stat"),
                        col("partition_nbr").as("partition_nbr"),
                        col("record_nbr").as("record_nbr"),
                        col("newline").as("newline")
                      ).getField("ctrx_d_carr").as("ctrx_d_carr")
                    )
                  )
                )
              ).getField("dxf_sk")
            )
            .cast(StringType)
        ).otherwise(
            when(
              isnull(
                callUDF(
                  "Regular_HK_UK_Lookup_Template_Acct",
                  ILM_Logical_Split_Files__foreign_key_lookup__Regular_Serial_Lookup__RFMT_Extract_Universal_Key_Reformat_ReinterpretAs_7235_Udf(
                    murmur(
                      struct(
                        struct(
                          col("ctrx_d_cag").as("ctrx_d_cag"),
                          col("ctrx_d_carr").as("ctrx_d_carr"),
                          col("c_d_carr_acc").as("c_d_carr_acc"),
                          col("dim_match_stat").as("dim_match_stat"),
                          col("partition_nbr").as("partition_nbr"),
                          col("record_nbr").as("record_nbr"),
                          col("newline").as("newline")
                        ).getField("c_d_carr_acc").as("c_d_carr_acc")
                      )
                    )
                  )
                ).getField("dxf_sk")
              ),
              lit(0)
            ).otherwise(
              callUDF(
                "Regular_HK_UK_Lookup_Template_Acct",
                ILM_Logical_Split_Files__foreign_key_lookup__Regular_Serial_Lookup__RFMT_Extract_Universal_Key_Reformat_ReinterpretAs_7235_Udf(
                  murmur(
                    struct(
                      struct(
                        col("ctrx_d_cag").as("ctrx_d_cag"),
                        col("ctrx_d_carr").as("ctrx_d_carr"),
                        col("c_d_carr_acc").as("c_d_carr_acc"),
                        col("dim_match_stat").as("dim_match_stat"),
                        col("partition_nbr").as("partition_nbr"),
                        col("record_nbr").as("record_nbr"),
                        col("newline").as("newline")
                      ).getField("c_d_carr_acc").as("c_d_carr_acc")
                    )
                  )
                )
              ).getField("dxf_sk")
            )
          )
          .cast(StringType)
          .as("carrier_acct_sk"),
        col("ctrx_d_cag.client_id").as("client_id"),
        col("ctrx_d_cag.carrier_id").as("carrier_id"),
        col("ctrx_d_cag.account_id").as("account_id"),
        col("ctrx_d_cag.employer_group_id").as("employer_group_id"),
        col("ctrx_d_cag.client_nm").as("client_nm"),
        col("ctrx_d_cag.carrier_nm").as("carrier_nm"),
        col("ctrx_d_cag.account_nm").as("account_nm"),
        col("ctrx_d_cag.employer_group_nm").as("employer_group_nm"),
        col("ctrx_d_cag.pvcy_excl_ind").cast(DecimalType(38, 0)).cast(StringType).as("pvcy_excl_ind"),
        col("ctrx_d_cag.pa_site_nm").as("pa_site_nm"),
        col("ctrx_d_cag.carrier_category_id").as("carrier_category_id"),
        col("ctrx_d_cag.carrier_category_desc").as("carrier_category_desc"),
        col("ctrx_d_cag.carrier_state_cd").as("carrier_state_cd"),
        col("ctrx_d_cag.acct_client_type_id").as("acct_client_type_id"),
        col("ctrx_d_cag.acct_client_type_desc").as("acct_client_type_desc"),
        col("ctrx_d_cag.acct_product_type_id").as("acct_product_type_id"),
        col("ctrx_d_cag.acct_product_type_desc").as("acct_product_type_desc"),
        col("ctrx_d_cag.acct_product_line_id").as("acct_product_line_id"),
        col("ctrx_d_cag.acct_product_line_desc").as("acct_product_line_desc"),
        col("ctrx_d_cag.contract_num").as("contract_num"),
        col("ctrx_d_cag.pbp_id").as("pbp_id"),
        col("ctrx_d_cag.gps_plan_cd").as("gps_plan_cd"),
        col("ctrx_d_cag.oracle_client_cd").as("oracle_client_cd"),
        col("ctrx_d_cag.claim_suffix_cd").as("claim_suffix_cd"),
        col("ctrx_d_cag.custom_no").as("custom_no"),
        col("ctrx_d_cag.funding_arrangment_cd").as("funding_arrangment_cd"),
        col("ctrx_d_cag.plan_variation_cd").as("plan_variation_cd"),
        col("ctrx_d_cag.policy_no").as("policy_no"),
        col("ctrx_d_cag.reporting_cd").as("reporting_cd"),
        col("ctrx_d_cag.product_cd").as("product_cd"),
        col("ctrx_d_cag.segment_ind").as("segment_ind"),
        col("ctrx_d_cag.hmo_acct_dvsn_cd").as("hmo_acct_dvsn_cd"),
        col("ctrx_d_cag.claim_account_cd").as("claim_account_cd"),
        col("ctrx_d_cag.sold_market_site_cd").as("sold_market_site_cd"),
        col("ctrx_d_cag.situs_state_cd").as("situs_state_cd"),
        col("ctrx_d_cag.platform_ind").as("platform_ind"),
        col("ctrx_d_cag.client_carrier_id").as("client_carrier_id"),
        col("ctrx_d_cag.shared_arrangement_ind").as("shared_arrangement_ind"),
        col("ctrx_d_cag.sales_office_cd").as("sales_office_cd"),
        col("ctrx_d_cag.accumulator_ind").as("accumulator_ind"),
        col("ctrx_d_cag.erisa_id").as("erisa_id"),
        col("ctrx_d_cag.renewal_dt").as("renewal_dt"),
        col("ctrx_d_cag.iplan_ind").as("iplan_ind"),
        col("ctrx_d_cag.legal_entity_cd").as("legal_entity_cd"),
        col("ctrx_d_cag.franchise_no").as("franchise_no"),
        col("ctrx_d_cag.lead_partner_cd").as("lead_partner_cd"),
        col("ctrx_d_cag.coc_cd").as("coc_cd"),
        col("ctrx_d_cag.migration_ind").cast(DecimalType(38, 0)).cast(StringType).as("migration_ind"),
        col("ctrx_d_cag.migration_effective_dt").as("migration_effective_dt"),
        col("ctrx_d_cag.product_classifier_id").as("product_classifier_id"),
        col("ctrx_d_cag.pharmacy_rider_cd").as("pharmacy_rider_cd"),
        col("ctrx_d_cag.cob_ind").as("cob_ind"),
        col("ctrx_d_cag.cob_effective_dt").as("cob_effective_dt"),
        col("ctrx_d_cag.transaction_ts").as("transaction_ts"),
        col("ctrx_d_cag.obligor_cd").as("obligor_cd"),
        col("ctrx_d_cag.speciality_ind").as("speciality_ind"),
        col("ctrx_d_cag.spclty_eff_dt").as("spclty_eff_dt"),
        col("ctrx_d_cag.pick_lob_cd").as("pick_lob_cd"),
        col("ctrx_d_cag.gl_product_cd").as("gl_product_cd"),
        col("ctrx_d_cag.gl_customer_cd").as("gl_customer_cd"),
        col("ctrx_d_cag.product_id").as("product_id"),
        col("ctrx_d_cag.hm_dlvry_only_ind").as("hm_dlvry_only_ind"),
        col("ctrx_d_cag.hm_dlvry_stat_ind").as("hm_dlvry_stat_ind"),
        col("ctrx_d_cag.rebates_only_ind").as("rebates_only_ind"),
        col("ctrx_d_cag.rebates_stat_ind").as("rebates_stat_ind"),
        col("ctrx_d_cag.src_env_sk").cast(DecimalType(38, 0)).cast(StringType).as("src_env_sk"),
        col("ctrx_d_cag.bseg_srv_typ").as("bseg_srv_typ"),
        col("ctrx_d_cag.bseg_bus_typ").as("bseg_bus_typ"),
        col("ctrx_d_cag.bseg_clt_typ").as("bseg_clt_typ"),
        col("ctrx_d_cag.busns_typ").as("busns_typ")
      ).as("ctrx_d_cag"),
      col("ctrx_d_carr").as("ctrx_d_carr"),
      col("c_d_carr_acc").as("c_d_carr_acc"),
      filter(
        array(
          when(
            when(
              isnull(
                callUDF(
                  "Regular_HK_UK_Lookup_Template",
                  ILM_Logical_Split_Files__foreign_key_lookup__Regular_Serial_Lookup__RFMT_Extract_Universal_Key_Reformat_ReinterpretAs_7235_Udf(
                    murmur(
                      struct(
                        struct(
                          col("ctrx_d_cag").as("ctrx_d_cag"),
                          col("ctrx_d_carr").as("ctrx_d_carr"),
                          col("c_d_carr_acc").as("c_d_carr_acc"),
                          col("dim_match_stat").as("dim_match_stat"),
                          col("partition_nbr").as("partition_nbr"),
                          col("record_nbr").as("record_nbr"),
                          col("newline").as("newline")
                        ).getField("ctrx_d_carr").as("ctrx_d_carr")
                      )
                    )
                  )
                ).getField("dxf_sk")
              ),
              lit(0)
            ).otherwise(
                callUDF(
                  "Regular_HK_UK_Lookup_Template",
                  ILM_Logical_Split_Files__foreign_key_lookup__Regular_Serial_Lookup__RFMT_Extract_Universal_Key_Reformat_ReinterpretAs_7235_Udf(
                    murmur(
                      struct(
                        struct(
                          col("ctrx_d_cag").as("ctrx_d_cag"),
                          col("ctrx_d_carr").as("ctrx_d_carr"),
                          col("c_d_carr_acc").as("c_d_carr_acc"),
                          col("dim_match_stat").as("dim_match_stat"),
                          col("partition_nbr").as("partition_nbr"),
                          col("record_nbr").as("record_nbr"),
                          col("newline").as("newline")
                        ).getField("ctrx_d_carr").as("ctrx_d_carr")
                      )
                    )
                  )
                ).getField("dxf_sk")
              )
              .cast(StringType) === lit(0),
            struct(
              lit("ids_common.d_carrier").as("dimension"),
              lit("ctrx_d_carr").as("nk_subrec"),
              lit(1).as("dim_match_flag"),
              ILM_Logical_Split_Files__foreign_key_lookup__Regular_Serial_Lookup__RFMT_Extract_Universal_Key_Reformat_ReinterpretAs_7235_Udf(
                murmur(
                  struct(
                    struct(
                      col("ctrx_d_cag").as("ctrx_d_cag"),
                      col("ctrx_d_carr").as("ctrx_d_carr"),
                      col("c_d_carr_acc").as("c_d_carr_acc"),
                      col("dim_match_stat").as("dim_match_stat"),
                      col("partition_nbr").as("partition_nbr"),
                      col("record_nbr").as("record_nbr"),
                      col("newline").as("newline")
                    ).getField("ctrx_d_carr").as("ctrx_d_carr")
                  )
                )
              ).as("fsk_hk")
            )
          ),
          when(
            (!(lit("ids_common.d_carrier_acct") === lit("ids_common.d_carrier")).and(
              ILM_Logical_Split_Files__foreign_key_lookup__Regular_Serial_Lookup__RFMT_Extract_Universal_Key_Reformat_ReinterpretAs_7235_Udf(
                murmur(
                  struct(
                    struct(
                      col("ctrx_d_cag").as("ctrx_d_cag"),
                      col("ctrx_d_carr").as("ctrx_d_carr"),
                      col("c_d_carr_acc").as("c_d_carr_acc"),
                      col("dim_match_stat").as("dim_match_stat"),
                      col("partition_nbr").as("partition_nbr"),
                      col("record_nbr").as("record_nbr"),
                      col("newline").as("newline")
                    ).getField("c_d_carr_acc").as("c_d_carr_acc")
                  )
                )
              ) === ILM_Logical_Split_Files__foreign_key_lookup__Regular_Serial_Lookup__RFMT_Extract_Universal_Key_Reformat_ReinterpretAs_7235_Udf(
                murmur(
                  struct(
                    struct(
                      col("ctrx_d_cag").as("ctrx_d_cag"),
                      col("ctrx_d_carr").as("ctrx_d_carr"),
                      col("c_d_carr_acc").as("c_d_carr_acc"),
                      col("dim_match_stat").as("dim_match_stat"),
                      col("partition_nbr").as("partition_nbr"),
                      col("record_nbr").as("record_nbr"),
                      col("newline").as("newline")
                    ).getField("ctrx_d_carr").as("ctrx_d_carr")
                  )
                )
              )
            )).and(
              when(
                isnull(
                  callUDF(
                    "Regular_HK_UK_Lookup_Template_Acct",
                    ILM_Logical_Split_Files__foreign_key_lookup__Regular_Serial_Lookup__RFMT_Extract_Universal_Key_Reformat_ReinterpretAs_7235_Udf(
                      murmur(
                        struct(
                          struct(
                            col("ctrx_d_cag").as("ctrx_d_cag"),
                            col("ctrx_d_carr").as("ctrx_d_carr"),
                            col("c_d_carr_acc").as("c_d_carr_acc"),
                            col("dim_match_stat").as("dim_match_stat"),
                            col("partition_nbr").as("partition_nbr"),
                            col("record_nbr").as("record_nbr"),
                            col("newline").as("newline")
                          ).getField("c_d_carr_acc").as("c_d_carr_acc")
                        )
                      )
                    )
                  ).getField("dxf_sk")
                ),
                lit(0)
              ).otherwise(
                callUDF(
                  "Regular_HK_UK_Lookup_Template_Acct",
                  ILM_Logical_Split_Files__foreign_key_lookup__Regular_Serial_Lookup__RFMT_Extract_Universal_Key_Reformat_ReinterpretAs_7235_Udf(
                    murmur(
                      struct(
                        struct(
                          col("ctrx_d_cag").as("ctrx_d_cag"),
                          col("ctrx_d_carr").as("ctrx_d_carr"),
                          col("c_d_carr_acc").as("c_d_carr_acc"),
                          col("dim_match_stat").as("dim_match_stat"),
                          col("partition_nbr").as("partition_nbr"),
                          col("record_nbr").as("record_nbr"),
                          col("newline").as("newline")
                        ).getField("c_d_carr_acc").as("c_d_carr_acc")
                      )
                    )
                  )
                ).getField("dxf_sk")
              ) === lit(0)
            ),
            struct(
              lit("ids_common.d_carrier_acct").as("dimension"),
              lit("c_d_carr_acc").as("nk_subrec"),
              lit(1).as("dim_match_flag"),
              ILM_Logical_Split_Files__foreign_key_lookup__Regular_Serial_Lookup__RFMT_Extract_Universal_Key_Reformat_ReinterpretAs_7235_Udf(
                murmur(
                  struct(
                    struct(
                      col("ctrx_d_cag").as("ctrx_d_cag"),
                      col("ctrx_d_carr").as("ctrx_d_carr"),
                      col("c_d_carr_acc").as("c_d_carr_acc"),
                      col("dim_match_stat").as("dim_match_stat"),
                      col("partition_nbr").as("partition_nbr"),
                      col("record_nbr").as("record_nbr"),
                      col("newline").as("newline")
                    ).getField("c_d_carr_acc").as("c_d_carr_acc")
                  )
                )
              ).as("fsk_hk")
            )
          )
        ),
        i ⇒ !isnull(i)
      ).as("dim_match_stat"),
      col("partition_nbr").as("partition_nbr"),
      col("record_nbr").as("record_nbr"),
      col("newline").as("newline")
    )

    out.persist(StorageLevel.MEMORY_AND_DISK)

  }

  val ILM_Logical_Split_Files__foreign_key_lookup__Regular_Serial_Lookup__RFMT_Extract_Universal_Key_Reformat_ReinterpretAs_7235_Udf =
    udf(
      (input: Any) => convertInputBytesToStructType(input, Array("long", "long")),
      StructType(
        List(
          StructField("dxf_hk_part1", LongType, true),
          StructField("dxf_hk_part2", LongType, true)
        )
      )
    )

}