package graph

import io.prophecy.libs.Component._
import io.prophecy.libs.SparkFunctions._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

@Visual(id = "ILM_Logical_Split_Files__ILM_SPLIT_0",
  label = "ILM_Logical_Split_Files.ILM_SPLIT_0",
  x = 100,
  y = 100,
  phase = 2
)
object ILM_Logical_Split_Files__ILM_SPLIT_0 {

  @UsesDataset(id = "6", version = 0)
  def apply(spark: SparkSession, in: DataFrame): Target = {
    val out = in.select(
      struct(
        col("ctrx_d_cag.is_equal").cast(DoubleType).as("is_equal"),
        col("ctrx_d_cag.drop_record").cast(DoubleType).as("drop_record"),
        to_timestamp(col("ctrx_d_cag.matching__eff_dt"), "yyyy-MM-dd HH:mm:ss").as("matching__eff_dt"),
        col("ctrx_d_cag.dxf_src_dataset_id").as("dxf_src_dataset_id"),
        col("ctrx_d_cag.dxf_src_rec_cnt").as("dxf_src_rec_cnt"),
        col("ctrx_d_cag.dxf_src_sys_id").cast(ShortType).as("dxf_src_sys_id"),
        col("ctrx_d_cag.dxf_src_file_name").as("dxf_src_file_name"),
        col("ctrx_d_cag.dxf_hk_part1").as("dxf_hk_part1"),
        col("ctrx_d_cag.dxf_hk_part2").as("dxf_hk_part2"),
        col("ctrx_d_cag.dxf_sk").as("dxf_sk"),
        col("ctrx_d_cag.cag_sk").cast(DoubleType).as("cag_sk"),
        col("ctrx_d_cag.dar_lob_sk").cast(DoubleType).as("dar_lob_sk"),
        col("ctrx_d_cag.acctg_lob_sk").cast(DoubleType).as("acctg_lob_sk"),
        col("ctrx_d_cag.client_sk").cast(DoubleType).as("client_sk"),
        col("ctrx_d_cag.carrier_sk").cast(DoubleType).as("carrier_sk"),
        col("ctrx_d_cag.carrier_acct_sk").cast(DoubleType).as("carrier_acct_sk"),
        replaceBlankColumnWithNull(col("ctrx_d_cag.client_id")).as("client_id"),
        replaceBlankColumnWithNull(col("ctrx_d_cag.carrier_id")).as("carrier_id"),
        replaceBlankColumnWithNull(col("ctrx_d_cag.account_id")).as("account_id"),
        replaceBlankColumnWithNull(col("ctrx_d_cag.employer_group_id")).as("employer_group_id"),
        replaceBlankColumnWithNull(col("ctrx_d_cag.client_nm")).as("client_nm"),
        replaceBlankColumnWithNull(col("ctrx_d_cag.carrier_nm")).as("carrier_nm"),
        replaceBlankColumnWithNull(col("ctrx_d_cag.account_nm")).as("account_nm"),
        replaceBlankColumnWithNull(col("ctrx_d_cag.employer_group_nm")).as("employer_group_nm"),
        col("ctrx_d_cag.pvcy_excl_ind").cast(DoubleType).as("pvcy_excl_ind"),
        replaceBlankColumnWithNull(col("ctrx_d_cag.pa_site_nm")).as("pa_site_nm"),
        replaceBlankColumnWithNull(col("ctrx_d_cag.carrier_category_id")).as("carrier_category_id"),
        replaceBlankColumnWithNull(col("ctrx_d_cag.carrier_category_desc")).as("carrier_category_desc"),
        replaceBlankColumnWithNull(col("ctrx_d_cag.carrier_state_cd")).as("carrier_state_cd"),
        replaceBlankColumnWithNull(col("ctrx_d_cag.acct_client_type_id")).as("acct_client_type_id"),
        replaceBlankColumnWithNull(col("ctrx_d_cag.acct_client_type_desc")).as("acct_client_type_desc"),
        replaceBlankColumnWithNull(col("ctrx_d_cag.acct_product_type_id")).as("acct_product_type_id"),
        replaceBlankColumnWithNull(col("ctrx_d_cag.acct_product_type_desc")).as("acct_product_type_desc"),
        replaceBlankColumnWithNull(col("ctrx_d_cag.acct_product_line_id")).as("acct_product_line_id"),
        replaceBlankColumnWithNull(col("ctrx_d_cag.acct_product_line_desc")).as("acct_product_line_desc"),
        replaceBlankColumnWithNull(col("ctrx_d_cag.contract_num")).as("contract_num"),
        replaceBlankColumnWithNull(col("ctrx_d_cag.pbp_id")).as("pbp_id"),
        replaceBlankColumnWithNull(col("ctrx_d_cag.gps_plan_cd")).as("gps_plan_cd"),
        replaceBlankColumnWithNull(col("ctrx_d_cag.oracle_client_cd")).as("oracle_client_cd"),
        replaceBlankColumnWithNull(col("ctrx_d_cag.claim_suffix_cd")).as("claim_suffix_cd"),
        replaceBlankColumnWithNull(col("ctrx_d_cag.custom_no")).as("custom_no"),
        replaceBlankColumnWithNull(col("ctrx_d_cag.funding_arrangment_cd")).as("funding_arrangment_cd"),
        replaceBlankColumnWithNull(col("ctrx_d_cag.plan_variation_cd")).as("plan_variation_cd"),
        replaceBlankColumnWithNull(col("ctrx_d_cag.policy_no")).as("policy_no"),
        replaceBlankColumnWithNull(col("ctrx_d_cag.reporting_cd")).as("reporting_cd"),
        replaceBlankColumnWithNull(col("ctrx_d_cag.product_cd")).as("product_cd"),
        replaceBlankColumnWithNull(col("ctrx_d_cag.segment_ind")).as("segment_ind"),
        replaceBlankColumnWithNull(col("ctrx_d_cag.hmo_acct_dvsn_cd")).as("hmo_acct_dvsn_cd"),
        replaceBlankColumnWithNull(col("ctrx_d_cag.claim_account_cd")).as("claim_account_cd"),
        replaceBlankColumnWithNull(col("ctrx_d_cag.sold_market_site_cd")).as("sold_market_site_cd"),
        replaceBlankColumnWithNull(col("ctrx_d_cag.situs_state_cd")).as("situs_state_cd"),
        replaceBlankColumnWithNull(col("ctrx_d_cag.platform_ind")).as("platform_ind"),
        replaceBlankColumnWithNull(col("ctrx_d_cag.client_carrier_id")).as("client_carrier_id"),
        replaceBlankColumnWithNull(col("ctrx_d_cag.shared_arrangement_ind")).as("shared_arrangement_ind"),
        replaceBlankColumnWithNull(col("ctrx_d_cag.sales_office_cd")).as("sales_office_cd"),
        replaceBlankColumnWithNull(col("ctrx_d_cag.accumulator_ind")).as("accumulator_ind"),
        replaceBlankColumnWithNull(col("ctrx_d_cag.erisa_id")).as("erisa_id"),
        to_date(col("ctrx_d_cag.renewal_dt"), "yyyyMMdd").as("renewal_dt"),
        replaceBlankColumnWithNull(col("ctrx_d_cag.iplan_ind")).as("iplan_ind"),
        replaceBlankColumnWithNull(col("ctrx_d_cag.legal_entity_cd")).as("legal_entity_cd"),
        replaceBlankColumnWithNull(col("ctrx_d_cag.franchise_no")).as("franchise_no"),
        replaceBlankColumnWithNull(col("ctrx_d_cag.lead_partner_cd")).as("lead_partner_cd"),
        replaceBlankColumnWithNull(col("ctrx_d_cag.coc_cd")).as("coc_cd"),
        col("ctrx_d_cag.migration_ind").cast(DoubleType).as("migration_ind"),
        to_date(col("ctrx_d_cag.migration_effective_dt"), "yyyyMMdd").as("migration_effective_dt"),
        replaceBlankColumnWithNull(col("ctrx_d_cag.product_classifier_id")).as("product_classifier_id"),
        replaceBlankColumnWithNull(col("ctrx_d_cag.pharmacy_rider_cd")).as("pharmacy_rider_cd"),
        replaceBlankColumnWithNull(col("ctrx_d_cag.cob_ind")).as("cob_ind"),
        to_date(col("ctrx_d_cag.cob_effective_dt"), "yyyyMMdd").as("cob_effective_dt"),
        to_timestamp(col("ctrx_d_cag.transaction_ts"), "yyyyMMddHHmmss").as("transaction_ts"),
        replaceBlankColumnWithNull(col("ctrx_d_cag.obligor_cd")).as("obligor_cd"),
        replaceBlankColumnWithNull(col("ctrx_d_cag.speciality_ind")).as("speciality_ind"),
        to_date(col("ctrx_d_cag.spclty_eff_dt"), "yyyyMMdd").as("spclty_eff_dt"),
        replaceBlankColumnWithNull(col("ctrx_d_cag.pick_lob_cd")).as("pick_lob_cd"),
        replaceBlankColumnWithNull(col("ctrx_d_cag.gl_product_cd")).as("gl_product_cd"),
        replaceBlankColumnWithNull(col("ctrx_d_cag.gl_customer_cd")).as("gl_customer_cd"),
        replaceBlankColumnWithNull(col("ctrx_d_cag.product_id")).as("product_id"),
        col("ctrx_d_cag.hm_dlvry_only_ind").as("hm_dlvry_only_ind"),
        col("ctrx_d_cag.hm_dlvry_stat_ind").as("hm_dlvry_stat_ind"),
        col("ctrx_d_cag.rebates_only_ind").as("rebates_only_ind"),
        col("ctrx_d_cag.rebates_stat_ind").as("rebates_stat_ind"),
        col("ctrx_d_cag.src_env_sk").cast(DoubleType).as("src_env_sk"),
        replaceBlankColumnWithNull(col("ctrx_d_cag.bseg_srv_typ")).as("bseg_srv_typ"),
        replaceBlankColumnWithNull(col("ctrx_d_cag.bseg_bus_typ")).as("bseg_bus_typ"),
        replaceBlankColumnWithNull(col("ctrx_d_cag.bseg_clt_typ")).as("bseg_clt_typ"),
        replaceBlankColumnWithNull(col("ctrx_d_cag.busns_typ")).as("busns_typ")
      ).as("ctrx_d_cag"),
      col("ctrx_d_carr").as("ctrx_d_carr"),
      col("c_d_carr_acc").as("c_d_carr_acc"),
      col("newline").as("newline")
    )

    out.write.mode(SaveMode.Overwrite).parquet(s"dbfs:/FileStore/optum/result/interim_orx_d_cag_ids_common_d_cag.parquet")

  }

}
