import graph._
import io.prophecy.libs.Component._
import org.apache.spark.sql._

@Visual(mode = "batch")
object Main {

  def graph(spark: SparkSession): Unit = {

    val df_d_cag_ind_lkp: Lookup = d_cag_ind_lkp(spark)
    val df_rccagp: Lookup = rccagp(spark)
    val df_Regular_HK_UK_Lookup_Template: Lookup = Regular_HK_UK_Lookup_Template(spark)
    val df_cag_gps_plan_code: Lookup = cag_gps_plan_code(spark)
    val df_lkp_rcagfp_orx_d_cag_custom: Lookup = lkp_rcagfp_orx_d_cag_custom(spark)
    val df_d_carrier_acct: Lookup = d_carrier_acct(spark)
    val df_Regular_HK_UK_Lookup_Template_Acct: Lookup = Regular_HK_UK_Lookup_Template_Acct(spark)
    val df_client_account_hierarchy: Lookup = client_account_hierarchy(spark)
    val df_client_id_cag_mask: Lookup = client_id_cag_mask(spark)
    val df_bob_180_cag_list_lkp: Lookup = bob_180_cag_list_lkp(spark)
    val df_d_carrier: Lookup = d_carrier(spark)
    val df_lkp_rcagdp: Lookup = lkp_rcagdp(spark)
    val df_ICFF_HK_UK_Lookup_Template: Lookup = ICFF_HK_UK_Lookup_Template(spark)
    val df_Clean_Source: Source = Clean_Source(spark)
    val (df_Optional_Filter__Optional_Filter_0, df_Optional_Filter__Optional_Filter_1): (
      RowDistributor,
        RowDistributor
      ) = Optional_Filter__Optional_Filter(spark, df_Clean_Source)
    val (df_Map_to_ILM_via_the_BRE__Reformat_xRouter_0, df_Map_to_ILM_via_the_BRE__Reformat_xRouter_1): (
      RowDistributor,
        RowDistributor
      ) = Map_to_ILM_via_the_BRE__Reformat_xRouter(spark, df_Optional_Filter__Optional_Filter_1)
    val df_Map_to_ILM_via_the_BRE__Reformat_Log: Select =
      Map_to_ILM_via_the_BRE__Reformat_Log(spark, df_Map_to_ILM_via_the_BRE__Reformat_xRouter_0)
    val df_nHandle_Logs: SetOperation = nHandle_Logs(spark, df_Map_to_ILM_via_the_BRE__Reformat_Log)
    val (df_Get_Count__Replicate_RowDistributor_0, df_Get_Count__Replicate_RowDistributor_1): (
      RowDistributor,
        RowDistributor
      ) = Get_Count__Replicate_RowDistributor(spark, df_nHandle_Logs)
    val df_Map_to_ILM_via_the_BRE__Reformat: Reformat =
      Map_to_ILM_via_the_BRE__Reformat(spark, df_Map_to_ILM_via_the_BRE__Reformat_xRouter_1)
    val df_Get_Count__Gather_1: SetOperation = Get_Count__Gather_1(spark, df_Get_Count__Replicate_RowDistributor_1)
    val df_Get_Count__FBE_Get_finish_event_types: Filter =
      Get_Count__FBE_Get_finish_event_types(spark, df_Get_Count__Gather_1)
    val df_Get_Count__RFMT_Get_Counts_all_partitions_Reformat: Reformat =
      Get_Count__RFMT_Get_Counts_all_partitions_Reformat(spark, df_Get_Count__FBE_Get_finish_event_types)
    val df_Get_Count__RollUp_Count_in_Serial: Aggregate =
      Get_Count__RollUp_Count_in_Serial(spark, df_Get_Count__RFMT_Get_Counts_all_partitions_Reformat)
    val df_ILM_Logical_Split_Files__Reformat_Reformat: Reformat =
      ILM_Logical_Split_Files__Reformat_Reformat(spark, df_Map_to_ILM_via_the_BRE__Reformat)
    val df_ILM_Logical_Split_Files__FBE_Drop_Records_with_drop_record_1: Filter =
      ILM_Logical_Split_Files__FBE_Drop_Records_with_drop_record_1(spark, df_ILM_Logical_Split_Files__Reformat_Reformat)
    val df_ILM_Logical_Split_Files__foreign_key_lookup__RFMT_Split_ICFF_and_Regular_Lookup_records_Reformat: Reformat =
      ILM_Logical_Split_Files__foreign_key_lookup__RFMT_Split_ICFF_and_Regular_Lookup_records_Reformat(
        spark,
        df_ILM_Logical_Split_Files__FBE_Drop_Records_with_drop_record_1
      )
    val (df_Get_Count__FBE_Split_each_count_flow_0, df_Get_Count__FBE_Split_each_count_flow_1): (
      RowDistributor,
        RowDistributor
      ) = Get_Count__FBE_Split_each_count_flow(spark, df_Get_Count__RollUp_Count_in_Serial)
    val df_ILM_Logical_Split_Files__foreign_key_lookup__Regular_Serial_Lookup__RFMT_Extract_Universal_Key_Reformat
    : Reformat =
      ILM_Logical_Split_Files__foreign_key_lookup__Regular_Serial_Lookup__RFMT_Extract_Universal_Key_Reformat(
        spark,
        df_ILM_Logical_Split_Files__foreign_key_lookup__RFMT_Split_ICFF_and_Regular_Lookup_records_Reformat
      )
    val (df_ILM_Logical_Split_Files__foreign_key_lookup__RFMT_Distinguish_Non_Matching_Records_RowDistributor_0,
    df_ILM_Logical_Split_Files__foreign_key_lookup__RFMT_Distinguish_Non_Matching_Records_RowDistributor_1
      ): (RowDistributor, RowDistributor) =
      ILM_Logical_Split_Files__foreign_key_lookup__RFMT_Distinguish_Non_Matching_Records_RowDistributor(
        spark,
        df_ILM_Logical_Split_Files__foreign_key_lookup__Regular_Serial_Lookup__RFMT_Extract_Universal_Key_Reformat
      )
    val df_ILM_Logical_Split_Files__foreign_key_lookup__RFMT_Distinguish_Non_Matching_RecordsReformat_0: Reformat =
      ILM_Logical_Split_Files__foreign_key_lookup__RFMT_Distinguish_Non_Matching_RecordsReformat_0(
        spark,
        df_ILM_Logical_Split_Files__foreign_key_lookup__RFMT_Distinguish_Non_Matching_Records_RowDistributor_1
      )
    ILM_Logical_Split_Files__ILM_SPLIT_0(
      spark,
      df_ILM_Logical_Split_Files__foreign_key_lookup__RFMT_Distinguish_Non_Matching_RecordsReformat_0
    )
    val df_ILM_Logical_Split_Files__foreign_key_lookup__RFMT_Distinguish_Non_Matching_RecordsReformat_1: Reformat =
      ILM_Logical_Split_Files__foreign_key_lookup__RFMT_Distinguish_Non_Matching_RecordsReformat_1(
        spark,
        df_ILM_Logical_Split_Files__foreign_key_lookup__RFMT_Distinguish_Non_Matching_Records_RowDistributor_0
      )
    val df_ILM_Logical_Split_Files__foreign_key_lookup__NORM_Split_non_matching_records_from_each_dimension: Normalize =
      ILM_Logical_Split_Files__foreign_key_lookup__NORM_Split_non_matching_records_from_each_dimension(
        spark,
        df_ILM_Logical_Split_Files__foreign_key_lookup__RFMT_Distinguish_Non_Matching_RecordsReformat_1
      )
    val df_ILM_Logical_Split_Files__Gather: SetOperation = ILM_Logical_Split_Files__Gather(
      spark,
      df_ILM_Logical_Split_Files__foreign_key_lookup__NORM_Split_non_matching_records_from_each_dimension
    )

    val (df_ILM_Logical_Split_Files__create_placeholder_files__RowDistributor_0,
    df_ILM_Logical_Split_Files__create_placeholder_files__RowDistributor_1,
    df_ILM_Logical_Split_Files__create_placeholder_files__RowDistributor_2): (RowDistributor, RowDistributor, RowDistributor) =
      ILM_Logical_Split_Files__create_placeholder_files__RowDistributor(spark, df_ILM_Logical_Split_Files__Gather)

    val df_ILM_Logical_Split_Files__create_placeholder_files__RFMT_Construct_record_for_inserting_in_dimension_Reformat
    : Reformat =
      ILM_Logical_Split_Files__create_placeholder_files__RFMT_Construct_record_for_inserting_in_dimension_Reformat(
        spark,
        df_ILM_Logical_Split_Files__create_placeholder_files__RowDistributor_2
      )
    ILM_Logical_Split_Files__create_placeholder_files__WMF_Write_dimension_placeholder_files(
      spark,
      df_ILM_Logical_Split_Files__create_placeholder_files__RFMT_Construct_record_for_inserting_in_dimension_Reformat
    )

    // rewritting previously written placeholder files in different format.
    ILM_Logical_Split_Files__create_placeholder_files__WMF_Write_carrier_dimension_placeholder_files(
      spark, df_ILM_Logical_Split_Files__create_placeholder_files__RowDistributor_0
    )
    ILM_Logical_Split_Files__create_placeholder_files__WMF_Write_carrier_acct_dimension_placeholder_files(
      spark, df_ILM_Logical_Split_Files__create_placeholder_files__RowDistributor_1
    )

    val df_Get_Count__Fuse_sequence1: Sequence =
      Get_Count__Fuse_sequence1(spark, df_Get_Count__FBE_Split_each_count_flow_0)
    val df_Get_Count__Fuse_sequence0: Sequence =
      Get_Count__Fuse_sequence0(spark, df_Get_Count__FBE_Split_each_count_flow_1)
    val df_Get_Count__Fuse: Join = Get_Count__Fuse(spark, df_Get_Count__Fuse_sequence1, df_Get_Count__Fuse_sequence0)
    Get_Count__Gather_Logs(spark, df_Get_Count__Replicate_RowDistributor_0)
    Optional_Filter__Deselect_File(spark, df_Optional_Filter__Optional_Filter_0)
    Get_Count__Audit_count_file(spark, df_Get_Count__Fuse)
  }

  def main(args: Array[String]): Unit = {
    import config._
    ConfigStore.Config = ConfigurationFactoryImpl.fromCLI(args)

    val spark: SparkSession = SparkSession
      .builder()
      .appName("software licensed from Ab Initio")
      .config("spark.default.parallelism", "4")
      .config("spark.sql.legacy.allowUntypedScalaUDF", "true")
      .enableHiveSupport()
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setCheckpointDir("/tmp/checkpoints")
    spark.conf.set("spark.sql.legacy.allowUntypedScalaUDF", true)
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
    graph(spark)
  }

}
