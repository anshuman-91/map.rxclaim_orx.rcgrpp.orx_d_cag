package graph

import io.prophecy.libs._
import io.prophecy.libs.SparkFunctions._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

@Visual(id = "Map_to_ILM_via_the_BRE__Reformat", label = "Map_to_ILM_via_the_BRE.Reformat", x = 100, y = 100, phase = 1)
object Map_to_ILM_via_the_BRE__Reformat {

  val AI_MIN_DATETIME = "1900-01-01 00:00:00"
  val AI_MAX_DATETIME = "9999-12-31 00:00:00"

  def apply(spark: SparkSession, in: DataFrame): Reformat = {

    val out = in.select(
      col("dxf_src_dataset_id").cast(LongType).as("dxf_src_dataset_id"),
      col("dxf_src_rec_cnt").cast(LongType).as("dxf_src_rec_cnt"),
      col("dxf_src_sys_id").cast(IntegerType).as("dxf_src_sys_id"),
      col("dxf_src_file_name").as("dxf_src_file_name"),
      struct(
        lit(0).cast(LongType).as("dxf_hk_part1"),
        lit(0).cast(LongType).as("dxf_hk_part2"),
        lit(0).cast(LongType).as("dxf_sk"),
        lit(-1).cast(DecimalType(38,                            0)).cast(StringType).as("cag_sk"),
        lit(-1).cast(DecimalType(38,                            0)).cast(StringType).as("dar_lob_sk"),
        lit(-1).cast(DecimalType(38,                            0)).cast(StringType).as("acctg_lob_sk"),
        lit(-1).cast(DecimalType(38,                            0)).cast(StringType).as("client_sk"),
        lit(-1).cast(DecimalType(38,                            0)).cast(StringType).as("carrier_sk"),
        lit(-1).cast(DecimalType(38,                            0)).cast(StringType).as("carrier_acct_sk"),
        when(isnull(tempExpression1375261_Udf()),               lit("OPTUM")).otherwise(tempExpression1375261_Udf()).as("client_id"),
        when(isnull(col("acaacd")).or(is_blank(col("acaacd"))), lit("-")).otherwise(col("acaacd")).as("carrier_id"),
        when(isnull(col("acaccd")).or(is_blank(col("acaccd"))), lit("-")).otherwise(col("acaccd")).as("account_id"),
        when(isnull(col("acadcd")).or(is_blank(col("acadcd"))), lit("-"))
          .otherwise(col("acadcd"))
          .as("employer_group_id"),
        when(isnull(tempExpression1375261_Udf()), lit("OPTUM")).otherwise(tempExpression1375261_Udf()).as("client_nm"),
        tempExpression1164145_Udf().getField("carrier_nm").as("carrier_nm"),
        callUDF(
          "d_carrier_acct",
          when(isnull(col("acaacd")).or(is_blank(col("acaacd"))), lit("-")).otherwise(col("acaacd")),
          when(isnull(col("acaccd")).or(is_blank(col("acaccd"))), lit("-")).otherwise(col("acaccd"))
        ).getField("account_nm").as("account_nm"),
        col("acattx").as("employer_group_nm"),
        when(trim(col("acaacd")).isin("ACTRCHSI", "080005"), lit(1))
          .otherwise(lit(0))
          .cast(DecimalType(38, 0))
          .cast(StringType)
          .cast(DecimalType(38, 0))
          .cast(StringType)
          .as("pvcy_excl_ind"),
        lit("-").as("pa_site_nm"),
        when(trim(col("acaacd")) === lit("PDPIND"), lit("C")).otherwise(lit("A")).as("carrier_category_id"),
        when(trim(col("acaacd")) === lit("PDPIND"), lit("C")).otherwise(lit("A")).as("carrier_category_desc"),
        tempExpression1164145_Udf().getField("carrier_state_cd").as("carrier_state_cd"),
        callUDF("client_account_hierarchy", col("acaacd"), col("acaccd"))
          .getField("acct_client_type_id")
          .as("acct_client_type_id"),
        callUDF("client_account_hierarchy", col("acaacd"), col("acaccd"))
          .getField("acct_client_type_desc")
          .as("acct_client_type_desc"),
        callUDF("client_account_hierarchy", col("acaacd"), col("acaccd"))
          .getField("acct_product_type_id")
          .as("acct_product_type_id"),
        callUDF("client_account_hierarchy", col("acaacd"), col("acaccd"))
          .getField("acct_product_type_desc")
          .as("acct_product_type_desc"),
        callUDF("client_account_hierarchy", col("acaacd"), col("acaccd"))
          .getField("acct_product_line_id")
          .as("acct_product_line_id"),
        callUDF("client_account_hierarchy", col("acaacd"), col("acaccd"))
          .getField("acct_product_line_desc")
          .as("acct_product_line_desc"),
        when(trim(col("acaacd")).isin("MPDOVA", "PDPIND"), string_substring(col("acadcd"), lit(2), lit(5)))
          .otherwise(lit("-"))
          .as("contract_num"),
        when(trim(col("acaacd")).isin("MPDOVA", "PDPIND"), string_substring(col("acaccd"), lit(4), lit(3)))
          .otherwise(lit("-"))
          .as("pbp_id"),
        when(
          !isnull(callUDF("cag_gps_plan_code", col("acaacd"), col("acaccd"), col("acadcd")).getField("gps_plan_code")),
          callUDF("cag_gps_plan_code", col("acaacd"), col("acaccd"), col("acadcd")).getField("gps_plan_code")
        ).otherwise(lit("UNK")).as("gps_plan_cd"),
        lit("-").as("oracle_client_cd"),
        when(
          !isnull(tempExpression1164489_Udf().getField("tot_str_length")),
          when(
            isnull(
              string_substring(tempExpression1375733_Udf(),
                               tempExpression1164489_Udf().getField("starting_position"),
                               tempExpression1164489_Udf().getField("tot_str_length")
              )
            ),
            lit("-")
          ).otherwise(
            string_substring(tempExpression1375733_Udf(),
                             tempExpression1164489_Udf().getField("starting_position"),
                             tempExpression1164489_Udf().getField("tot_str_length")
            )
          )
        ).otherwise(
            when(
              !isnull(tempExpression1164743_Udf().getField("tot_str_length")),
              when(
                isnull(
                  string_substring(tempExpression1375733_Udf(),
                                   tempExpression1164743_Udf().getField("starting_position"),
                                   tempExpression1164743_Udf().getField("tot_str_length")
                  )
                ),
                lit("-")
              ).otherwise(
                string_substring(tempExpression1375733_Udf(),
                                 tempExpression1164743_Udf().getField("starting_position"),
                                 tempExpression1164743_Udf().getField("tot_str_length")
                )
              )
            ).otherwise(tempExpression2278243_Udf())
          )
          .as("claim_suffix_cd"),
        when(
          !isnull(tempExpression1165978_Udf().getField("tot_str_length")),
          when(
            isnull(
              string_substring(tempExpression1375733_Udf(),
                               tempExpression1165978_Udf().getField("starting_position"),
                               tempExpression1165978_Udf().getField("tot_str_length")
              )
            ),
            lit("-")
          ).otherwise(
            string_substring(tempExpression1375733_Udf(),
                             tempExpression1165978_Udf().getField("starting_position"),
                             tempExpression1165978_Udf().getField("tot_str_length")
            )
          )
        ).otherwise(
            when(
              !isnull(tempExpression1166232_Udf().getField("tot_str_length")),
              when(
                isnull(
                  string_substring(tempExpression1375733_Udf(),
                                   tempExpression1166232_Udf().getField("starting_position"),
                                   tempExpression1166232_Udf().getField("tot_str_length")
                  )
                ),
                lit("-")
              ).otherwise(
                string_substring(tempExpression1375733_Udf(),
                                 tempExpression1166232_Udf().getField("starting_position"),
                                 tempExpression1166232_Udf().getField("tot_str_length")
                )
              )
            ).otherwise(tempExpression2278695_Udf())
          )
          .as("custom_no"),
        tempExpression2293651_Udf().as("funding_arrangment_cd"),
        when(
          !isnull(tempExpression1169052_Udf().getField("tot_str_length")),
          when(
            isnull(
              string_substring(tempExpression1375733_Udf(),
                               tempExpression1169052_Udf().getField("starting_position"),
                               tempExpression1169052_Udf().getField("tot_str_length")
              )
            ),
            lit("-")
          ).otherwise(
            string_substring(tempExpression1375733_Udf(),
                             tempExpression1169052_Udf().getField("starting_position"),
                             tempExpression1169052_Udf().getField("tot_str_length")
            )
          )
        ).otherwise(
            when(
              !isnull(tempExpression1169306_Udf().getField("tot_str_length")),
              when(
                isnull(
                  string_substring(tempExpression1375733_Udf(),
                                   tempExpression1169306_Udf().getField("starting_position"),
                                   tempExpression1169306_Udf().getField("tot_str_length")
                  )
                ),
                lit("-")
              ).otherwise(
                string_substring(tempExpression1375733_Udf(),
                                 tempExpression1169306_Udf().getField("starting_position"),
                                 tempExpression1169306_Udf().getField("tot_str_length")
                )
              )
            ).otherwise(
              when(
                !isnull(tempExpression1169551_Udf().getField("tot_str_length")),
                when(
                  isnull(
                    string_substring(tempExpression1375733_Udf(),
                                     tempExpression1169551_Udf().getField("starting_position"),
                                     tempExpression1169551_Udf().getField("tot_str_length")
                    )
                  ),
                  lit("-")
                ).otherwise(
                  string_substring(tempExpression1375733_Udf(),
                                   tempExpression1169551_Udf().getField("starting_position"),
                                   tempExpression1169551_Udf().getField("tot_str_length")
                  )
                )
              ).otherwise(lit("-"))
            )
          )
          .as("plan_variation_cd"),
        when(
          (col("dxf_src_sys_id").cast(IntegerType) === lit(490)).and(
            !isnull(
              callUDF(
                "lkp_rcagfp_orx_d_cag_custom",
                trim(col("acaacd")),
                trim(col("acaccd")),
                trim(col("acadcd")),
                lit("POLICY NUMBER"),
                col("dxf_src_sys_id").cast(IntegerType)
              ).getField("tot_str_length")
            )
          ),
          when(isnull(string_substring(col("acadcd"), lit(1), lit(7))), lit("-"))
            .otherwise(string_substring(col("acadcd"), lit(1), lit(7)))
        ).otherwise(tempExpression2279919_Udf()).as("policy_no"),
        when(
          !isnull(tempExpression1170248_Udf().getField("tot_str_length")),
          when(
            isnull(
              string_substring(tempExpression1375733_Udf(),
                               tempExpression1170248_Udf().getField("starting_position"),
                               tempExpression1170248_Udf().getField("tot_str_length")
              )
            ),
            lit("-")
          ).otherwise(
            string_substring(tempExpression1375733_Udf(),
                             tempExpression1170248_Udf().getField("starting_position"),
                             tempExpression1170248_Udf().getField("tot_str_length")
            )
          )
        ).otherwise(
            when(
              !isnull(tempExpression1170502_Udf().getField("tot_str_length")),
              when(
                isnull(
                  string_substring(tempExpression1375733_Udf(),
                                   tempExpression1170502_Udf().getField("starting_position"),
                                   tempExpression1170502_Udf().getField("tot_str_length")
                  )
                ),
                lit("-")
              ).otherwise(
                string_substring(tempExpression1375733_Udf(),
                                 tempExpression1170502_Udf().getField("starting_position"),
                                 tempExpression1170502_Udf().getField("tot_str_length")
                )
              )
            ).otherwise(
              when(
                !isnull(tempExpression1170747_Udf().getField("tot_str_length")),
                when(
                  isnull(
                    string_substring(tempExpression1375733_Udf(),
                                     tempExpression1170747_Udf().getField("starting_position"),
                                     tempExpression1170747_Udf().getField("tot_str_length")
                    )
                  ),
                  lit("-")
                ).otherwise(
                  string_substring(tempExpression1375733_Udf(),
                                   tempExpression1170747_Udf().getField("starting_position"),
                                   tempExpression1170747_Udf().getField("tot_str_length")
                  )
                )
              ).otherwise(lit("-"))
            )
          )
          .as("reporting_cd"),
        when(
          !isnull(tempExpression1170993_Udf().getField("tot_str_length")),
          when(
            isnull(
              string_substring(tempExpression1375733_Udf(),
                               tempExpression1170993_Udf().getField("starting_position"),
                               tempExpression1170993_Udf().getField("tot_str_length")
              )
            ),
            lit("-")
          ).otherwise(
            string_substring(tempExpression1375733_Udf(),
                             tempExpression1170993_Udf().getField("starting_position"),
                             tempExpression1170993_Udf().getField("tot_str_length")
            )
          )
        ).otherwise(
            when(
              !isnull(tempExpression1171247_Udf().getField("tot_str_length")),
              when(
                isnull(
                  string_substring(tempExpression1375733_Udf(),
                                   tempExpression1171247_Udf().getField("starting_position"),
                                   tempExpression1171247_Udf().getField("tot_str_length")
                  )
                ),
                lit("-")
              ).otherwise(
                string_substring(tempExpression1375733_Udf(),
                                 tempExpression1171247_Udf().getField("starting_position"),
                                 tempExpression1171247_Udf().getField("tot_str_length")
                )
              )
            ).otherwise(
              when(
                !isnull(tempExpression1171492_Udf().getField("tot_str_length")),
                when(
                  isnull(
                    string_substring(tempExpression1375733_Udf(),
                                     tempExpression1171492_Udf().getField("starting_position"),
                                     tempExpression1171492_Udf().getField("tot_str_length")
                    )
                  ),
                  lit("-")
                ).otherwise(
                  string_substring(tempExpression1375733_Udf(),
                                   tempExpression1171492_Udf().getField("starting_position"),
                                   tempExpression1171492_Udf().getField("tot_str_length")
                  )
                )
              ).otherwise(lit("-"))
            )
          )
          .as("product_cd"),
        when(
          !isnull(tempExpression1171738_Udf().getField("tot_str_length")),
          when(
            isnull(
              string_substring(tempExpression1375733_Udf(),
                               tempExpression1171738_Udf().getField("starting_position"),
                               tempExpression1171738_Udf().getField("tot_str_length")
              )
            ),
            lit("-")
          ).otherwise(
            string_substring(tempExpression1375733_Udf(),
                             tempExpression1171738_Udf().getField("starting_position"),
                             tempExpression1171738_Udf().getField("tot_str_length")
            )
          )
        ).otherwise(
            when(
              !isnull(tempExpression1171992_Udf().getField("tot_str_length")),
              when(
                isnull(
                  string_substring(tempExpression1375733_Udf(),
                                   tempExpression1171992_Udf().getField("starting_position"),
                                   tempExpression1171992_Udf().getField("tot_str_length")
                  )
                ),
                lit("-")
              ).otherwise(
                string_substring(tempExpression1375733_Udf(),
                                 tempExpression1171992_Udf().getField("starting_position"),
                                 tempExpression1171992_Udf().getField("tot_str_length")
                )
              )
            ).otherwise(tempExpression2280925_Udf())
          )
          .as("segment_ind"),
        when(
          !isnull(tempExpression1173227_Udf().getField("tot_str_length")),
          when(
            isnull(
              string_substring(tempExpression1375733_Udf(),
                               tempExpression1173227_Udf().getField("starting_position"),
                               tempExpression1173227_Udf().getField("tot_str_length")
              )
            ),
            lit("-")
          ).otherwise(
            string_substring(tempExpression1375733_Udf(),
                             tempExpression1173227_Udf().getField("starting_position"),
                             tempExpression1173227_Udf().getField("tot_str_length")
            )
          )
        ).otherwise(
            when(
              !isnull(tempExpression1173481_Udf().getField("tot_str_length")),
              when(
                isnull(
                  string_substring(tempExpression1375733_Udf(),
                                   tempExpression1173481_Udf().getField("starting_position"),
                                   tempExpression1173481_Udf().getField("tot_str_length")
                  )
                ),
                lit("-")
              ).otherwise(
                string_substring(tempExpression1375733_Udf(),
                                 tempExpression1173481_Udf().getField("starting_position"),
                                 tempExpression1173481_Udf().getField("tot_str_length")
                )
              )
            ).otherwise(
              when(
                !isnull(tempExpression1173726_Udf().getField("tot_str_length")),
                when(
                  isnull(
                    string_substring(tempExpression1375733_Udf(),
                                     tempExpression1173726_Udf().getField("starting_position"),
                                     tempExpression1173726_Udf().getField("tot_str_length")
                    )
                  ),
                  lit("-")
                ).otherwise(
                  string_substring(tempExpression1375733_Udf(),
                                   tempExpression1173726_Udf().getField("starting_position"),
                                   tempExpression1173726_Udf().getField("tot_str_length")
                  )
                )
              ).otherwise(lit(null))
            )
          )
          .as("hmo_acct_dvsn_cd"),
        when(
          !isnull(tempExpression1173972_Udf().getField("tot_str_length")),
          when(
            isnull(
              string_substring(tempExpression1375733_Udf(),
                               tempExpression1173972_Udf().getField("starting_position"),
                               tempExpression1173972_Udf().getField("tot_str_length")
              )
            ),
            lit("-")
          ).otherwise(
            string_substring(tempExpression1375733_Udf(),
                             tempExpression1173972_Udf().getField("starting_position"),
                             tempExpression1173972_Udf().getField("tot_str_length")
            )
          )
        ).otherwise(
            when(
              !isnull(tempExpression1174226_Udf().getField("tot_str_length")),
              when(
                isnull(
                  string_substring(tempExpression1375733_Udf(),
                                   tempExpression1174226_Udf().getField("starting_position"),
                                   tempExpression1174226_Udf().getField("tot_str_length")
                  )
                ),
                lit("-")
              ).otherwise(
                string_substring(tempExpression1375733_Udf(),
                                 tempExpression1174226_Udf().getField("starting_position"),
                                 tempExpression1174226_Udf().getField("tot_str_length")
                )
              )
            ).otherwise(
              when(
                !isnull(tempExpression1174471_Udf().getField("tot_str_length")),
                when(
                  isnull(
                    string_substring(tempExpression1375733_Udf(),
                                     tempExpression1174471_Udf().getField("starting_position"),
                                     tempExpression1174471_Udf().getField("tot_str_length")
                    )
                  ),
                  lit("-")
                ).otherwise(
                  string_substring(tempExpression1375733_Udf(),
                                   tempExpression1174471_Udf().getField("starting_position"),
                                   tempExpression1174471_Udf().getField("tot_str_length")
                  )
                )
              ).otherwise(lit(null))
            )
          )
          .as("claim_account_cd"),
        when(
          !isnull(tempExpression1174717_Udf().getField("tot_str_length")),
          when(
            isnull(
              string_substring(tempExpression1375733_Udf(),
                               tempExpression1174717_Udf().getField("starting_position"),
                               tempExpression1174717_Udf().getField("tot_str_length")
              )
            ),
            lit("-")
          ).otherwise(
            string_substring(tempExpression1375733_Udf(),
                             tempExpression1174717_Udf().getField("starting_position"),
                             tempExpression1174717_Udf().getField("tot_str_length")
            )
          )
        ).otherwise(
            when(
              !isnull(tempExpression1174971_Udf().getField("tot_str_length")),
              when(
                isnull(
                  string_substring(tempExpression1375733_Udf(),
                                   tempExpression1174971_Udf().getField("starting_position"),
                                   tempExpression1174971_Udf().getField("tot_str_length")
                  )
                ),
                lit("-")
              ).otherwise(
                string_substring(tempExpression1375733_Udf(),
                                 tempExpression1174971_Udf().getField("starting_position"),
                                 tempExpression1174971_Udf().getField("tot_str_length")
                )
              )
            ).otherwise(tempExpression2281951_Udf())
          )
          .as("sold_market_site_cd"),
        when(
          !isnull(tempExpression1176206_Udf().getField("tot_str_length")),
          when(
            isnull(
              string_substring(tempExpression1375733_Udf(),
                               tempExpression1176206_Udf().getField("starting_position"),
                               tempExpression1176206_Udf().getField("tot_str_length")
              )
            ),
            lit("-")
          ).otherwise(
            string_substring(tempExpression1375733_Udf(),
                             tempExpression1176206_Udf().getField("starting_position"),
                             tempExpression1176206_Udf().getField("tot_str_length")
            )
          )
        ).otherwise(
            when(
              !isnull(tempExpression1176460_Udf().getField("tot_str_length")),
              when(
                isnull(
                  string_substring(tempExpression1375733_Udf(),
                                   tempExpression1176460_Udf().getField("starting_position"),
                                   tempExpression1176460_Udf().getField("tot_str_length")
                  )
                ),
                lit("-")
              ).otherwise(
                string_substring(tempExpression1375733_Udf(),
                                 tempExpression1176460_Udf().getField("starting_position"),
                                 tempExpression1176460_Udf().getField("tot_str_length")
                )
              )
            ).otherwise(
              when(
                !isnull(tempExpression1176705_Udf().getField("tot_str_length")),
                when(
                  isnull(
                    string_substring(tempExpression1375733_Udf(),
                                     tempExpression1176705_Udf().getField("starting_position"),
                                     tempExpression1176705_Udf().getField("tot_str_length")
                    )
                  ),
                  lit("-")
                ).otherwise(
                  string_substring(tempExpression1375733_Udf(),
                                   tempExpression1176705_Udf().getField("starting_position"),
                                   tempExpression1176705_Udf().getField("tot_str_length")
                  )
                )
              ).otherwise(lit(null))
            )
          )
          .as("situs_state_cd"),
        when(
          !isnull(tempExpression1176951_Udf().getField("tot_str_length")),
          when(
            isnull(
              string_substring(tempExpression1375733_Udf(),
                               tempExpression1176951_Udf().getField("starting_position"),
                               tempExpression1176951_Udf().getField("tot_str_length")
              )
            ),
            lit("-")
          ).otherwise(
            string_substring(tempExpression1375733_Udf(),
                             tempExpression1176951_Udf().getField("starting_position"),
                             tempExpression1176951_Udf().getField("tot_str_length")
            )
          )
        ).otherwise(
            when(
              !isnull(tempExpression1177205_Udf().getField("tot_str_length")),
              when(
                isnull(
                  string_substring(tempExpression1375733_Udf(),
                                   tempExpression1177205_Udf().getField("starting_position"),
                                   tempExpression1177205_Udf().getField("tot_str_length")
                  )
                ),
                lit("-")
              ).otherwise(
                string_substring(tempExpression1375733_Udf(),
                                 tempExpression1177205_Udf().getField("starting_position"),
                                 tempExpression1177205_Udf().getField("tot_str_length")
                )
              )
            ).otherwise(
              when(
                !isnull(tempExpression1177450_Udf().getField("tot_str_length")),
                when(
                  isnull(
                    string_substring(tempExpression1375733_Udf(),
                                     tempExpression1177450_Udf().getField("starting_position"),
                                     tempExpression1177450_Udf().getField("tot_str_length")
                    )
                  ),
                  lit("-")
                ).otherwise(
                  string_substring(tempExpression1375733_Udf(),
                                   tempExpression1177450_Udf().getField("starting_position"),
                                   tempExpression1177450_Udf().getField("tot_str_length")
                  )
                )
              ).otherwise(lit(null))
            )
          )
          .as("platform_ind"),
        when(
          !isnull(tempExpression1177696_Udf().getField("tot_str_length")),
          when(
            isnull(
              string_substring(tempExpression1375733_Udf(),
                               tempExpression1177696_Udf().getField("starting_position"),
                               tempExpression1177696_Udf().getField("tot_str_length")
              )
            ),
            lit("-")
          ).otherwise(
            string_substring(tempExpression1375733_Udf(),
                             tempExpression1177696_Udf().getField("starting_position"),
                             tempExpression1177696_Udf().getField("tot_str_length")
            )
          )
        ).otherwise(
            when(
              !isnull(tempExpression1177950_Udf().getField("tot_str_length")),
              when(
                isnull(
                  string_substring(tempExpression1375733_Udf(),
                                   tempExpression1177950_Udf().getField("starting_position"),
                                   tempExpression1177950_Udf().getField("tot_str_length")
                  )
                ),
                lit("-")
              ).otherwise(
                string_substring(tempExpression1375733_Udf(),
                                 tempExpression1177950_Udf().getField("starting_position"),
                                 tempExpression1177950_Udf().getField("tot_str_length")
                )
              )
            ).otherwise(
              when(
                !isnull(tempExpression1178195_Udf().getField("tot_str_length")),
                when(
                  isnull(
                    string_substring(tempExpression1375733_Udf(),
                                     tempExpression1178195_Udf().getField("starting_position"),
                                     tempExpression1178195_Udf().getField("tot_str_length")
                    )
                  ),
                  lit("-")
                ).otherwise(
                  string_substring(tempExpression1375733_Udf(),
                                   tempExpression1178195_Udf().getField("starting_position"),
                                   tempExpression1178195_Udf().getField("tot_str_length")
                  )
                )
              ).otherwise(lit(null))
            )
          )
          .as("client_carrier_id"),
        when(
          !isnull(tempExpression1178441_Udf().getField("tot_str_length")),
          when(
            isnull(
              string_substring(tempExpression1375733_Udf(),
                               tempExpression1178441_Udf().getField("starting_position"),
                               tempExpression1178441_Udf().getField("tot_str_length")
              )
            ),
            lit("-")
          ).otherwise(
            string_substring(tempExpression1375733_Udf(),
                             tempExpression1178441_Udf().getField("starting_position"),
                             tempExpression1178441_Udf().getField("tot_str_length")
            )
          )
        ).otherwise(
            when(
              !isnull(tempExpression1178695_Udf().getField("tot_str_length")),
              when(
                isnull(
                  string_substring(tempExpression1375733_Udf(),
                                   tempExpression1178695_Udf().getField("starting_position"),
                                   tempExpression1178695_Udf().getField("tot_str_length")
                  )
                ),
                lit("-")
              ).otherwise(
                string_substring(tempExpression1375733_Udf(),
                                 tempExpression1178695_Udf().getField("starting_position"),
                                 tempExpression1178695_Udf().getField("tot_str_length")
                )
              )
            ).otherwise(
              when(
                !isnull(tempExpression1178940_Udf().getField("tot_str_length")),
                when(
                  isnull(
                    string_substring(tempExpression1375733_Udf(),
                                     tempExpression1178940_Udf().getField("starting_position"),
                                     tempExpression1178940_Udf().getField("tot_str_length")
                    )
                  ),
                  lit("-")
                ).otherwise(
                  string_substring(tempExpression1375733_Udf(),
                                   tempExpression1178940_Udf().getField("starting_position"),
                                   tempExpression1178940_Udf().getField("tot_str_length")
                  )
                )
              ).otherwise(lit(null))
            )
          )
          .as("shared_arrangement_ind"),
        when(
          !isnull(tempExpression1179186_Udf().getField("tot_str_length")),
          when(
            isnull(
              string_substring(tempExpression1375733_Udf(),
                               tempExpression1179186_Udf().getField("starting_position"),
                               tempExpression1179186_Udf().getField("tot_str_length")
              )
            ),
            lit("-")
          ).otherwise(
            string_substring(tempExpression1375733_Udf(),
                             tempExpression1179186_Udf().getField("starting_position"),
                             tempExpression1179186_Udf().getField("tot_str_length")
            )
          )
        ).otherwise(
            when(
              !isnull(tempExpression1179440_Udf().getField("tot_str_length")),
              when(
                isnull(
                  string_substring(tempExpression1375733_Udf(),
                                   tempExpression1179440_Udf().getField("starting_position"),
                                   tempExpression1179440_Udf().getField("tot_str_length")
                  )
                ),
                lit("-")
              ).otherwise(
                string_substring(tempExpression1375733_Udf(),
                                 tempExpression1179440_Udf().getField("starting_position"),
                                 tempExpression1179440_Udf().getField("tot_str_length")
                )
              )
            ).otherwise(
              when(
                !isnull(tempExpression1179685_Udf().getField("tot_str_length")),
                when(
                  isnull(
                    string_substring(tempExpression1375733_Udf(),
                                     tempExpression1179685_Udf().getField("starting_position"),
                                     tempExpression1179685_Udf().getField("tot_str_length")
                    )
                  ),
                  lit("-")
                ).otherwise(
                  string_substring(tempExpression1375733_Udf(),
                                   tempExpression1179685_Udf().getField("starting_position"),
                                   tempExpression1179685_Udf().getField("tot_str_length")
                  )
                )
              ).otherwise(lit(null))
            )
          )
          .as("sales_office_cd"),
        when(
          !isnull(tempExpression1179931_Udf().getField("tot_str_length")),
          when(
            isnull(
              string_substring(tempExpression1375733_Udf(),
                               tempExpression1179931_Udf().getField("starting_position"),
                               tempExpression1179931_Udf().getField("tot_str_length")
              )
            ),
            lit("-")
          ).otherwise(
            string_substring(tempExpression1375733_Udf(),
                             tempExpression1179931_Udf().getField("starting_position"),
                             tempExpression1179931_Udf().getField("tot_str_length")
            )
          )
        ).otherwise(
            when(
              !isnull(tempExpression1180185_Udf().getField("tot_str_length")),
              when(
                isnull(
                  string_substring(tempExpression1375733_Udf(),
                                   tempExpression1180185_Udf().getField("starting_position"),
                                   tempExpression1180185_Udf().getField("tot_str_length")
                  )
                ),
                lit("-")
              ).otherwise(
                string_substring(tempExpression1375733_Udf(),
                                 tempExpression1180185_Udf().getField("starting_position"),
                                 tempExpression1180185_Udf().getField("tot_str_length")
                )
              )
            ).otherwise(
              when(
                !isnull(tempExpression1180430_Udf().getField("tot_str_length")),
                when(
                  isnull(
                    string_substring(tempExpression1375733_Udf(),
                                     tempExpression1180430_Udf().getField("starting_position"),
                                     tempExpression1180430_Udf().getField("tot_str_length")
                    )
                  ),
                  lit("-")
                ).otherwise(
                  string_substring(tempExpression1375733_Udf(),
                                   tempExpression1180430_Udf().getField("starting_position"),
                                   tempExpression1180430_Udf().getField("tot_str_length")
                  )
                )
              ).otherwise(lit(null))
            )
          )
          .as("accumulator_ind"),
        when(
          !isnull(tempExpression1180676_Udf().getField("tot_str_length")),
          when(
            isnull(
              string_substring(tempExpression1375733_Udf(),
                               tempExpression1180676_Udf().getField("starting_position"),
                               tempExpression1180676_Udf().getField("tot_str_length")
              )
            ),
            lit("-")
          ).otherwise(
            string_substring(tempExpression1375733_Udf(),
                             tempExpression1180676_Udf().getField("starting_position"),
                             tempExpression1180676_Udf().getField("tot_str_length")
            )
          )
        ).otherwise(
            when(
              !isnull(tempExpression1180930_Udf().getField("tot_str_length")),
              when(
                isnull(
                  string_substring(tempExpression1375733_Udf(),
                                   tempExpression1180930_Udf().getField("starting_position"),
                                   tempExpression1180930_Udf().getField("tot_str_length")
                  )
                ),
                lit("-")
              ).otherwise(
                string_substring(tempExpression1375733_Udf(),
                                 tempExpression1180930_Udf().getField("starting_position"),
                                 tempExpression1180930_Udf().getField("tot_str_length")
                )
              )
            ).otherwise(
              when(
                !isnull(tempExpression1181175_Udf().getField("tot_str_length")),
                when(
                  isnull(
                    string_substring(tempExpression1375733_Udf(),
                                     tempExpression1181175_Udf().getField("starting_position"),
                                     tempExpression1181175_Udf().getField("tot_str_length")
                    )
                  ),
                  lit("-")
                ).otherwise(
                  string_substring(tempExpression1375733_Udf(),
                                   tempExpression1181175_Udf().getField("starting_position"),
                                   tempExpression1181175_Udf().getField("tot_str_length")
                  )
                )
              ).otherwise(lit(null))
            )
          )
          .as("erisa_id"),
        date_format(
          to_date(
            when(
              !when(tempExpression1525847_Udf().cast(BooleanType), lit(1).cast(BooleanType))
                .otherwise(
                  when(tempExpression1525892_Udf().cast(BooleanType), lit(1).cast(BooleanType))
                    .otherwise(lit(0).cast(BooleanType))
                )
                .cast(BooleanType),
              date_format(to_timestamp(lit(s"${AI_MAX_DATETIME}").cast(StringType), "yyyy-MM-dd HH:mm:ss"),
                          "yyyyMMdd"
              )
            ).otherwise(
                date_format(
                  to_date(
                    when(tempExpression1525847_Udf().cast(BooleanType), tempExpression1508408_Udf())
                      .otherwise(
                        when(
                          tempExpression1525892_Udf().cast(BooleanType),
                          when(
                            length(trim(tempExpression1490296_Udf().cast(StringType).cast(StringType))) === lit(8),
                            date_format(to_date(trim(tempExpression1490296_Udf().cast(StringType).cast(StringType))
                                                  .cast(StringType)
                                                  .cast(StringType),
                                                "yyyyMMdd"
                                        ),
                                        "yyyyMMdd"
                            )
                          )
                        ).otherwise(date_format(to_date(lit(null).cast(StringType), "yyyyMMdd"), "yyyyMMdd"))
                      )
                      .cast(StringType),
                    "yyyyMMdd"
                  ),
                  "yyyyMMdd"
                )
              )
              .cast(StringType),
            "yyyyMMdd"
          ),
          "yyyyMMdd"
        ).as("renewal_dt"),
        when(
          !isnull(tempExpression1205395_Udf().getField("tot_str_length")),
          when(
            isnull(
              string_substring(tempExpression1375733_Udf(),
                               tempExpression1205395_Udf().getField("starting_position"),
                               tempExpression1205395_Udf().getField("tot_str_length")
              )
            ),
            lit("-")
          ).otherwise(
            string_substring(tempExpression1375733_Udf(),
                             tempExpression1205395_Udf().getField("starting_position"),
                             tempExpression1205395_Udf().getField("tot_str_length")
            )
          )
        ).otherwise(
            when(
              !isnull(tempExpression1205649_Udf().getField("tot_str_length")),
              when(
                isnull(
                  string_substring(tempExpression1375733_Udf(),
                                   tempExpression1205649_Udf().getField("starting_position"),
                                   tempExpression1205649_Udf().getField("tot_str_length")
                  )
                ),
                lit("-")
              ).otherwise(
                string_substring(tempExpression1375733_Udf(),
                                 tempExpression1205649_Udf().getField("starting_position"),
                                 tempExpression1205649_Udf().getField("tot_str_length")
                )
              )
            ).otherwise(
              when(
                !isnull(tempExpression1205894_Udf().getField("tot_str_length")),
                when(
                  isnull(
                    string_substring(tempExpression1375733_Udf(),
                                     tempExpression1205894_Udf().getField("starting_position"),
                                     tempExpression1205894_Udf().getField("tot_str_length")
                    )
                  ),
                  lit("-")
                ).otherwise(
                  string_substring(tempExpression1375733_Udf(),
                                   tempExpression1205894_Udf().getField("starting_position"),
                                   tempExpression1205894_Udf().getField("tot_str_length")
                  )
                )
              ).otherwise(lit(null))
            )
          )
          .as("iplan_ind"),
        when(
          !isnull(tempExpression1206140_Udf().getField("tot_str_length")),
          when(
            isnull(
              string_substring(tempExpression1375733_Udf(),
                               tempExpression1206140_Udf().getField("starting_position"),
                               tempExpression1206140_Udf().getField("tot_str_length")
              )
            ),
            lit("-")
          ).otherwise(
            string_substring(tempExpression1375733_Udf(),
                             tempExpression1206140_Udf().getField("starting_position"),
                             tempExpression1206140_Udf().getField("tot_str_length")
            )
          )
        ).otherwise(
            when(
              !isnull(tempExpression1206394_Udf().getField("tot_str_length")),
              when(
                isnull(
                  string_substring(tempExpression1375733_Udf(),
                                   tempExpression1206394_Udf().getField("starting_position"),
                                   tempExpression1206394_Udf().getField("tot_str_length")
                  )
                ),
                lit("-")
              ).otherwise(
                string_substring(tempExpression1375733_Udf(),
                                 tempExpression1206394_Udf().getField("starting_position"),
                                 tempExpression1206394_Udf().getField("tot_str_length")
                )
              )
            ).otherwise(
              when(
                !isnull(tempExpression1206639_Udf().getField("tot_str_length")),
                when(
                  isnull(
                    string_substring(tempExpression1375733_Udf(),
                                     tempExpression1206639_Udf().getField("starting_position"),
                                     tempExpression1206639_Udf().getField("tot_str_length")
                    )
                  ),
                  lit("-")
                ).otherwise(
                  string_substring(tempExpression1375733_Udf(),
                                   tempExpression1206639_Udf().getField("starting_position"),
                                   tempExpression1206639_Udf().getField("tot_str_length")
                  )
                )
              ).otherwise(lit(null))
            )
          )
          .as("legal_entity_cd"),
        when(
          !isnull(tempExpression1206885_Udf().getField("tot_str_length")),
          when(
            isnull(
              string_substring(tempExpression1375733_Udf(),
                               tempExpression1206885_Udf().getField("starting_position"),
                               tempExpression1206885_Udf().getField("tot_str_length")
              )
            ),
            lit("-")
          ).otherwise(
            string_substring(tempExpression1375733_Udf(),
                             tempExpression1206885_Udf().getField("starting_position"),
                             tempExpression1206885_Udf().getField("tot_str_length")
            )
          )
        ).otherwise(
            when(
              !isnull(tempExpression1207139_Udf().getField("tot_str_length")),
              when(
                isnull(
                  string_substring(tempExpression1375733_Udf(),
                                   tempExpression1207139_Udf().getField("starting_position"),
                                   tempExpression1207139_Udf().getField("tot_str_length")
                  )
                ),
                lit("-")
              ).otherwise(
                string_substring(tempExpression1375733_Udf(),
                                 tempExpression1207139_Udf().getField("starting_position"),
                                 tempExpression1207139_Udf().getField("tot_str_length")
                )
              )
            ).otherwise(
              when(
                !isnull(tempExpression1207384_Udf().getField("tot_str_length")),
                when(
                  isnull(
                    string_substring(tempExpression1375733_Udf(),
                                     tempExpression1207384_Udf().getField("starting_position"),
                                     tempExpression1207384_Udf().getField("tot_str_length")
                    )
                  ),
                  lit("-")
                ).otherwise(
                  string_substring(tempExpression1375733_Udf(),
                                   tempExpression1207384_Udf().getField("starting_position"),
                                   tempExpression1207384_Udf().getField("tot_str_length")
                  )
                )
              ).otherwise(lit(null))
            )
          )
          .as("franchise_no"),
        when(
          !isnull(tempExpression1207630_Udf().getField("tot_str_length")),
          when(
            isnull(
              string_substring(tempExpression1375733_Udf(),
                               tempExpression1207630_Udf().getField("starting_position"),
                               tempExpression1207630_Udf().getField("tot_str_length")
              )
            ),
            lit("-")
          ).otherwise(
            string_substring(tempExpression1375733_Udf(),
                             tempExpression1207630_Udf().getField("starting_position"),
                             tempExpression1207630_Udf().getField("tot_str_length")
            )
          )
        ).otherwise(
            when(
              !isnull(tempExpression1207884_Udf().getField("tot_str_length")),
              when(
                isnull(
                  string_substring(tempExpression1375733_Udf(),
                                   tempExpression1207884_Udf().getField("starting_position"),
                                   tempExpression1207884_Udf().getField("tot_str_length")
                  )
                ),
                lit("-")
              ).otherwise(
                string_substring(tempExpression1375733_Udf(),
                                 tempExpression1207884_Udf().getField("starting_position"),
                                 tempExpression1207884_Udf().getField("tot_str_length")
                )
              )
            ).otherwise(
              when(
                !isnull(tempExpression1208129_Udf().getField("tot_str_length")),
                when(
                  isnull(
                    string_substring(tempExpression1375733_Udf(),
                                     tempExpression1208129_Udf().getField("starting_position"),
                                     tempExpression1208129_Udf().getField("tot_str_length")
                    )
                  ),
                  lit("-")
                ).otherwise(
                  string_substring(tempExpression1375733_Udf(),
                                   tempExpression1208129_Udf().getField("starting_position"),
                                   tempExpression1208129_Udf().getField("tot_str_length")
                  )
                )
              ).otherwise(lit(null))
            )
          )
          .as("lead_partner_cd"),
        when(
          !isnull(tempExpression1208375_Udf().getField("tot_str_length")),
          when(
            isnull(
              string_substring(tempExpression1375733_Udf(),
                               tempExpression1208375_Udf().getField("starting_position"),
                               tempExpression1208375_Udf().getField("tot_str_length")
              )
            ),
            lit("-")
          ).otherwise(
            string_substring(tempExpression1375733_Udf(),
                             tempExpression1208375_Udf().getField("starting_position"),
                             tempExpression1208375_Udf().getField("tot_str_length")
            )
          )
        ).otherwise(
            when(
              !isnull(tempExpression1208629_Udf().getField("tot_str_length")),
              when(
                isnull(
                  string_substring(tempExpression1375733_Udf(),
                                   tempExpression1208629_Udf().getField("starting_position"),
                                   tempExpression1208629_Udf().getField("tot_str_length")
                  )
                ),
                lit("-")
              ).otherwise(
                string_substring(tempExpression1375733_Udf(),
                                 tempExpression1208629_Udf().getField("starting_position"),
                                 tempExpression1208629_Udf().getField("tot_str_length")
                )
              )
            ).otherwise(
              when(
                !isnull(tempExpression1208874_Udf().getField("tot_str_length")),
                when(
                  isnull(
                    string_substring(tempExpression1375733_Udf(),
                                     tempExpression1208874_Udf().getField("starting_position"),
                                     tempExpression1208874_Udf().getField("tot_str_length")
                    )
                  ),
                  lit("-")
                ).otherwise(
                  string_substring(tempExpression1375733_Udf(),
                                   tempExpression1208874_Udf().getField("starting_position"),
                                   tempExpression1208874_Udf().getField("tot_str_length")
                  )
                )
              ).otherwise(lit(null))
            )
          )
          .as("coc_cd"),
        when(
          isnull(
            when(!isnull(tempExpression1209120_Udf().getField("tot_str_length")), tempExpression1420556_Udf())
              .otherwise(tempExpression1449035_Udf())
          ),
          lit(null)
        ).otherwise(
            when(
              when(!isnull(tempExpression1209120_Udf().getField("tot_str_length")), tempExpression1420556_Udf())
                .otherwise(tempExpression1449035_Udf()) === lit("Y"),
              lit(1)
            ).otherwise(
              when(
                when(!isnull(tempExpression1209120_Udf().getField("tot_str_length")), tempExpression1420556_Udf())
                  .otherwise(tempExpression1449035_Udf()) === lit("N"),
                lit(0)
              ).otherwise(lit(null))
            )
          )
          .cast(DecimalType(38, 0))
          .cast(StringType)
          .cast(DecimalType(38, 0))
          .cast(StringType)
          .as("migration_ind"),
        date_format(
          to_date(
            when(
              !when(
                when(isnull(tempExpression1472050_Udf().cast(BooleanType).and(tempExpression1493150_Udf() === lit(1))),
                     lit(0).cast(BooleanType)
                ).otherwise(tempExpression1472050_Udf().cast(BooleanType).and(tempExpression1493150_Udf() === lit(1))),
                lit(1).cast(BooleanType)
              ).otherwise(
                  when(tempExpression1493199_Udf().cast(BooleanType), lit(1).cast(BooleanType))
                    .otherwise(lit(0).cast(BooleanType))
                )
                .cast(BooleanType),
              date_format(to_timestamp(lit(s"${AI_MIN_DATETIME}").cast(StringType), "yyyy-MM-dd HH:mm:ss"),
                          "yyyyMMdd"
              )
            ).otherwise(
                date_format(
                  to_date(
                    when(
                      when(isnull(
                             tempExpression1472050_Udf().cast(BooleanType).and(tempExpression1493150_Udf() === lit(1))
                           ),
                           lit(0).cast(BooleanType)
                      ).otherwise(
                        tempExpression1472050_Udf().cast(BooleanType).and(tempExpression1493150_Udf() === lit(1))
                      ),
                      when(
                        length(trim(tempExpression1472115_Udf().cast(StringType))) === lit(8),
                        date_format(
                          to_date(trim(tempExpression1472115_Udf().cast(StringType)).cast(StringType).cast(StringType),
                                  "yyyyMMdd"
                          ),
                          "yyyyMMdd"
                        )
                      )
                    ).otherwise(
                        when(tempExpression1493199_Udf().cast(BooleanType),
                             when(tempExpression1472343_Udf().cast(BooleanType), tempExpression1472432_Udf())
                        ).otherwise(date_format(to_date(lit(null).cast(StringType), "yyyyMMdd"), "yyyyMMdd"))
                      )
                      .cast(StringType),
                    "yyyyMMdd"
                  ),
                  "yyyyMMdd"
                )
              )
              .cast(StringType),
            "yyyyMMdd"
          ),
          "yyyyMMdd"
        ).as("migration_effective_dt"),
        when(
          !isnull(tempExpression1223431_Udf().getField("tot_str_length")),
          when(
            isnull(
              string_substring(tempExpression1375733_Udf(),
                               tempExpression1223431_Udf().getField("starting_position"),
                               tempExpression1223431_Udf().getField("tot_str_length")
              )
            ),
            lit("-")
          ).otherwise(
            string_substring(tempExpression1375733_Udf(),
                             tempExpression1223431_Udf().getField("starting_position"),
                             tempExpression1223431_Udf().getField("tot_str_length")
            )
          )
        ).otherwise(
            when(
              !isnull(tempExpression1223685_Udf().getField("tot_str_length")),
              when(
                isnull(
                  string_substring(tempExpression1375733_Udf(),
                                   tempExpression1223685_Udf().getField("starting_position"),
                                   tempExpression1223685_Udf().getField("tot_str_length")
                  )
                ),
                lit("-")
              ).otherwise(
                string_substring(tempExpression1375733_Udf(),
                                 tempExpression1223685_Udf().getField("starting_position"),
                                 tempExpression1223685_Udf().getField("tot_str_length")
                )
              )
            ).otherwise(
              when(
                !isnull(tempExpression1223930_Udf().getField("tot_str_length")),
                when(
                  isnull(
                    string_substring(tempExpression1375733_Udf(),
                                     tempExpression1223930_Udf().getField("starting_position"),
                                     tempExpression1223930_Udf().getField("tot_str_length")
                    )
                  ),
                  lit("-")
                ).otherwise(
                  string_substring(tempExpression1375733_Udf(),
                                   tempExpression1223930_Udf().getField("starting_position"),
                                   tempExpression1223930_Udf().getField("tot_str_length")
                  )
                )
              ).otherwise(lit(null))
            )
          )
          .as("product_classifier_id"),
        when(
          !isnull(tempExpression1224176_Udf().getField("tot_str_length")),
          when(
            isnull(
              string_substring(tempExpression1375733_Udf(),
                               tempExpression1224176_Udf().getField("starting_position"),
                               tempExpression1224176_Udf().getField("tot_str_length")
              )
            ),
            lit("-")
          ).otherwise(
            string_substring(tempExpression1375733_Udf(),
                             tempExpression1224176_Udf().getField("starting_position"),
                             tempExpression1224176_Udf().getField("tot_str_length")
            )
          )
        ).otherwise(
            when(
              !isnull(tempExpression1224430_Udf().getField("tot_str_length")),
              when(
                isnull(
                  string_substring(tempExpression1375733_Udf(),
                                   tempExpression1224430_Udf().getField("starting_position"),
                                   tempExpression1224430_Udf().getField("tot_str_length")
                  )
                ),
                lit("-")
              ).otherwise(
                string_substring(tempExpression1375733_Udf(),
                                 tempExpression1224430_Udf().getField("starting_position"),
                                 tempExpression1224430_Udf().getField("tot_str_length")
                )
              )
            ).otherwise(
              when(
                !isnull(tempExpression1224675_Udf().getField("tot_str_length")),
                when(
                  isnull(
                    string_substring(tempExpression1375733_Udf(),
                                     tempExpression1224675_Udf().getField("starting_position"),
                                     tempExpression1224675_Udf().getField("tot_str_length")
                    )
                  ),
                  lit("-")
                ).otherwise(
                  string_substring(tempExpression1375733_Udf(),
                                   tempExpression1224675_Udf().getField("starting_position"),
                                   tempExpression1224675_Udf().getField("tot_str_length")
                  )
                )
              ).otherwise(lit(null))
            )
          )
          .as("pharmacy_rider_cd"),
        when(
          !isnull(tempExpression1224921_Udf().getField("tot_str_length")),
          when(
            isnull(
              string_substring(tempExpression1375733_Udf(),
                               tempExpression1224921_Udf().getField("starting_position"),
                               tempExpression1224921_Udf().getField("tot_str_length")
              )
            ),
            lit("-")
          ).otherwise(
            string_substring(tempExpression1375733_Udf(),
                             tempExpression1224921_Udf().getField("starting_position"),
                             tempExpression1224921_Udf().getField("tot_str_length")
            )
          )
        ).otherwise(
            when(
              !isnull(tempExpression1225175_Udf().getField("tot_str_length")),
              when(
                isnull(
                  string_substring(tempExpression1375733_Udf(),
                                   tempExpression1225175_Udf().getField("starting_position"),
                                   tempExpression1225175_Udf().getField("tot_str_length")
                  )
                ),
                lit("-")
              ).otherwise(
                string_substring(tempExpression1375733_Udf(),
                                 tempExpression1225175_Udf().getField("starting_position"),
                                 tempExpression1225175_Udf().getField("tot_str_length")
                )
              )
            ).otherwise(
              when(
                !isnull(tempExpression1225420_Udf().getField("tot_str_length")),
                when(
                  isnull(
                    string_substring(tempExpression1375733_Udf(),
                                     tempExpression1225420_Udf().getField("starting_position"),
                                     tempExpression1225420_Udf().getField("tot_str_length")
                    )
                  ),
                  lit("-")
                ).otherwise(
                  string_substring(tempExpression1375733_Udf(),
                                   tempExpression1225420_Udf().getField("starting_position"),
                                   tempExpression1225420_Udf().getField("tot_str_length")
                  )
                )
              ).otherwise(lit(null))
            )
          )
          .as("cob_ind"),
        date_format(
          to_date(
            when(
              !when(
                when(isnull(tempExpression1474142_Udf().cast(BooleanType).and(tempExpression1494293_Udf() === lit(1))),
                     lit(0).cast(BooleanType)
                ).otherwise(tempExpression1474142_Udf().cast(BooleanType).and(tempExpression1494293_Udf() === lit(1))),
                lit(1).cast(BooleanType)
              ).otherwise(
                  when(tempExpression1494342_Udf().cast(BooleanType), lit(1).cast(BooleanType))
                    .otherwise(lit(0).cast(BooleanType))
                )
                .cast(BooleanType),
              date_format(to_timestamp(lit(s"${AI_MIN_DATETIME}").cast(StringType), "yyyy-MM-dd HH:mm:ss"),
                          "yyyyMMdd"
              )
            ).otherwise(
                date_format(
                  to_date(
                    when(
                      when(isnull(
                             tempExpression1474142_Udf().cast(BooleanType).and(tempExpression1494293_Udf() === lit(1))
                           ),
                           lit(0).cast(BooleanType)
                      ).otherwise(
                        tempExpression1474142_Udf().cast(BooleanType).and(tempExpression1494293_Udf() === lit(1))
                      ),
                      when(
                        length(trim(tempExpression1474207_Udf().cast(StringType))) === lit(8),
                        date_format(
                          to_date(trim(tempExpression1474207_Udf().cast(StringType)).cast(StringType).cast(StringType),
                                  "yyyyMMdd"
                          ),
                          "yyyyMMdd"
                        )
                      )
                    ).otherwise(
                        when(tempExpression1494342_Udf().cast(BooleanType),
                             when(tempExpression1474435_Udf().cast(BooleanType), tempExpression1474524_Udf())
                        ).otherwise(date_format(to_date(lit(null).cast(StringType), "yyyyMMdd"), "yyyyMMdd"))
                      )
                      .cast(StringType),
                    "yyyyMMdd"
                  ),
                  "yyyyMMdd"
                )
              )
              .cast(StringType),
            "yyyyMMdd"
          ),
          "yyyyMMdd"
        ).as("cob_effective_dt"),
        tempExpression2287947_Udf().as("transaction_ts"),
        when(
          !isnull(tempExpression1248913_Udf().getField("tot_str_length")),
          when(
            isnull(
              string_substring(tempExpression1375733_Udf(),
                               tempExpression1248913_Udf().getField("starting_position"),
                               tempExpression1248913_Udf().getField("tot_str_length")
              )
            ),
            lit("-")
          ).otherwise(
            string_substring(tempExpression1375733_Udf(),
                             tempExpression1248913_Udf().getField("starting_position"),
                             tempExpression1248913_Udf().getField("tot_str_length")
            )
          )
        ).otherwise(
            when(
              !isnull(tempExpression1249167_Udf().getField("tot_str_length")),
              when(
                isnull(
                  string_substring(tempExpression1375733_Udf(),
                                   tempExpression1249167_Udf().getField("starting_position"),
                                   tempExpression1249167_Udf().getField("tot_str_length")
                  )
                ),
                lit("-")
              ).otherwise(
                string_substring(tempExpression1375733_Udf(),
                                 tempExpression1249167_Udf().getField("starting_position"),
                                 tempExpression1249167_Udf().getField("tot_str_length")
                )
              )
            ).otherwise(tempExpression2288393_Udf())
          )
          .as("obligor_cd"),
        when(
          !isnull(tempExpression1250402_Udf().getField("tot_str_length")),
          when(
            isnull(
              string_substring(tempExpression1375733_Udf(),
                               tempExpression1250402_Udf().getField("starting_position"),
                               tempExpression1250402_Udf().getField("tot_str_length")
              )
            ),
            lit("-")
          ).otherwise(
            string_substring(tempExpression1375733_Udf(),
                             tempExpression1250402_Udf().getField("starting_position"),
                             tempExpression1250402_Udf().getField("tot_str_length")
            )
          )
        ).otherwise(
            when(
              !isnull(tempExpression1250656_Udf().getField("tot_str_length")),
              when(
                isnull(
                  string_substring(tempExpression1375733_Udf(),
                                   tempExpression1250656_Udf().getField("starting_position"),
                                   tempExpression1250656_Udf().getField("tot_str_length")
                  )
                ),
                lit("-")
              ).otherwise(
                string_substring(tempExpression1375733_Udf(),
                                 tempExpression1250656_Udf().getField("starting_position"),
                                 tempExpression1250656_Udf().getField("tot_str_length")
                )
              )
            ).otherwise(
              when(
                !isnull(tempExpression1250901_Udf().getField("tot_str_length")),
                when(
                  isnull(
                    string_substring(tempExpression1375733_Udf(),
                                     tempExpression1250901_Udf().getField("starting_position"),
                                     tempExpression1250901_Udf().getField("tot_str_length")
                    )
                  ),
                  lit("-")
                ).otherwise(
                  string_substring(tempExpression1375733_Udf(),
                                   tempExpression1250901_Udf().getField("starting_position"),
                                   tempExpression1250901_Udf().getField("tot_str_length")
                  )
                )
              ).otherwise(lit(null))
            )
          )
          .as("speciality_ind"),
        date_format(
          to_date(
            when(
              !when(
                when(isnull(tempExpression1477174_Udf().cast(BooleanType).and(tempExpression1496186_Udf() === lit(1))),
                     lit(0).cast(BooleanType)
                ).otherwise(tempExpression1477174_Udf().cast(BooleanType).and(tempExpression1496186_Udf() === lit(1))),
                lit(1).cast(BooleanType)
              ).otherwise(
                  when(tempExpression1496235_Udf().cast(BooleanType), lit(1).cast(BooleanType))
                    .otherwise(lit(0).cast(BooleanType))
                )
                .cast(BooleanType),
              date_format(to_timestamp(lit(s"${AI_MIN_DATETIME}").cast(StringType), "yyyy-MM-dd HH:mm:ss"),
                          "yyyyMMdd"
              )
            ).otherwise(
                date_format(
                  to_date(
                    when(
                      when(isnull(
                             tempExpression1477174_Udf().cast(BooleanType).and(tempExpression1496186_Udf() === lit(1))
                           ),
                           lit(0).cast(BooleanType)
                      ).otherwise(
                        tempExpression1477174_Udf().cast(BooleanType).and(tempExpression1496186_Udf() === lit(1))
                      ),
                      when(
                        length(trim(tempExpression1477239_Udf().cast(StringType))) === lit(8),
                        date_format(
                          to_date(trim(tempExpression1477239_Udf().cast(StringType)).cast(StringType).cast(StringType),
                                  "yyyyMMdd"
                          ),
                          "yyyyMMdd"
                        )
                      )
                    ).otherwise(
                        when(tempExpression1496235_Udf().cast(BooleanType),
                             when(tempExpression1477467_Udf().cast(BooleanType), tempExpression1477556_Udf())
                        ).otherwise(date_format(to_date(lit(null).cast(StringType), "yyyyMMdd"), "yyyyMMdd"))
                      )
                      .cast(StringType),
                    "yyyyMMdd"
                  ),
                  "yyyyMMdd"
                )
              )
              .cast(StringType),
            "yyyyMMdd"
          ),
          "yyyyMMdd"
        ).as("spclty_eff_dt"),
        when(
          !isnull(tempExpression1263217_Udf().getField("tot_str_length")),
          when(
            isnull(
              string_substring(tempExpression1375733_Udf(),
                               tempExpression1263217_Udf().getField("starting_position"),
                               tempExpression1263217_Udf().getField("tot_str_length")
              )
            ),
            lit("-")
          ).otherwise(
            string_substring(tempExpression1375733_Udf(),
                             tempExpression1263217_Udf().getField("starting_position"),
                             tempExpression1263217_Udf().getField("tot_str_length")
            )
          )
        ).otherwise(
            when(
              !isnull(tempExpression1263471_Udf().getField("tot_str_length")),
              when(
                isnull(
                  string_substring(tempExpression1375733_Udf(),
                                   tempExpression1263471_Udf().getField("starting_position"),
                                   tempExpression1263471_Udf().getField("tot_str_length")
                  )
                ),
                lit("-")
              ).otherwise(
                string_substring(tempExpression1375733_Udf(),
                                 tempExpression1263471_Udf().getField("starting_position"),
                                 tempExpression1263471_Udf().getField("tot_str_length")
                )
              )
            ).otherwise(tempExpression2289468_Udf())
          )
          .as("pick_lob_cd"),
        when(
          !isnull(tempExpression1264706_Udf().getField("tot_str_length")),
          when(
            isnull(
              string_substring(tempExpression1375733_Udf(),
                               tempExpression1264706_Udf().getField("starting_position"),
                               tempExpression1264706_Udf().getField("tot_str_length")
              )
            ),
            lit("-")
          ).otherwise(
            string_substring(tempExpression1375733_Udf(),
                             tempExpression1264706_Udf().getField("starting_position"),
                             tempExpression1264706_Udf().getField("tot_str_length")
            )
          )
        ).otherwise(
            when(
              !isnull(tempExpression1264960_Udf().getField("tot_str_length")),
              when(
                isnull(
                  string_substring(tempExpression1375733_Udf(),
                                   tempExpression1264960_Udf().getField("starting_position"),
                                   tempExpression1264960_Udf().getField("tot_str_length")
                  )
                ),
                lit("-")
              ).otherwise(
                string_substring(tempExpression1375733_Udf(),
                                 tempExpression1264960_Udf().getField("starting_position"),
                                 tempExpression1264960_Udf().getField("tot_str_length")
                )
              )
            ).otherwise(
              when(
                !isnull(tempExpression1265205_Udf().getField("tot_str_length")),
                when(
                  isnull(
                    string_substring(tempExpression1375733_Udf(),
                                     tempExpression1265205_Udf().getField("starting_position"),
                                     tempExpression1265205_Udf().getField("tot_str_length")
                    )
                  ),
                  lit("-")
                ).otherwise(
                  string_substring(tempExpression1375733_Udf(),
                                   tempExpression1265205_Udf().getField("starting_position"),
                                   tempExpression1265205_Udf().getField("tot_str_length")
                  )
                )
              ).otherwise(lit(null))
            )
          )
          .as("gl_product_cd"),
        when(
          !isnull(tempExpression1265451_Udf().getField("tot_str_length")),
          when(
            isnull(
              string_substring(tempExpression1375733_Udf(),
                               tempExpression1265451_Udf().getField("starting_position"),
                               tempExpression1265451_Udf().getField("tot_str_length")
              )
            ),
            lit("-")
          ).otherwise(
            string_substring(tempExpression1375733_Udf(),
                             tempExpression1265451_Udf().getField("starting_position"),
                             tempExpression1265451_Udf().getField("tot_str_length")
            )
          )
        ).otherwise(
            when(
              !isnull(tempExpression1265705_Udf().getField("tot_str_length")),
              when(
                isnull(
                  string_substring(tempExpression1375733_Udf(),
                                   tempExpression1265705_Udf().getField("starting_position"),
                                   tempExpression1265705_Udf().getField("tot_str_length")
                  )
                ),
                lit("-")
              ).otherwise(
                string_substring(tempExpression1375733_Udf(),
                                 tempExpression1265705_Udf().getField("starting_position"),
                                 tempExpression1265705_Udf().getField("tot_str_length")
                )
              )
            ).otherwise(
              when(
                !isnull(tempExpression1265950_Udf().getField("tot_str_length")),
                when(
                  isnull(
                    string_substring(tempExpression1375733_Udf(),
                                     tempExpression1265950_Udf().getField("starting_position"),
                                     tempExpression1265950_Udf().getField("tot_str_length")
                    )
                  ),
                  lit("-")
                ).otherwise(
                  string_substring(tempExpression1375733_Udf(),
                                   tempExpression1265950_Udf().getField("starting_position"),
                                   tempExpression1265950_Udf().getField("tot_str_length")
                  )
                )
              ).otherwise(lit(null))
            )
          )
          .as("gl_customer_cd"),
        when(
          !isnull(tempExpression1266196_Udf().getField("tot_str_length")),
          when(
            isnull(
              string_substring(tempExpression1375733_Udf(),
                               tempExpression1266196_Udf().getField("starting_position"),
                               tempExpression1266196_Udf().getField("tot_str_length")
              )
            ),
            lit("-")
          ).otherwise(
            string_substring(tempExpression1375733_Udf(),
                             tempExpression1266196_Udf().getField("starting_position"),
                             tempExpression1266196_Udf().getField("tot_str_length")
            )
          )
        ).otherwise(
            when(
              !isnull(tempExpression1266450_Udf().getField("tot_str_length")),
              when(
                isnull(
                  string_substring(tempExpression1375733_Udf(),
                                   tempExpression1266450_Udf().getField("starting_position"),
                                   tempExpression1266450_Udf().getField("tot_str_length")
                  )
                ),
                lit("-")
              ).otherwise(
                string_substring(tempExpression1375733_Udf(),
                                 tempExpression1266450_Udf().getField("starting_position"),
                                 tempExpression1266450_Udf().getField("tot_str_length")
                )
              )
            ).otherwise(
              when(
                !isnull(tempExpression1266695_Udf().getField("tot_str_length")),
                when(
                  isnull(
                    string_substring(tempExpression1375733_Udf(),
                                     tempExpression1266695_Udf().getField("starting_position"),
                                     tempExpression1266695_Udf().getField("tot_str_length")
                    )
                  ),
                  lit("-")
                ).otherwise(
                  string_substring(tempExpression1375733_Udf(),
                                   tempExpression1266695_Udf().getField("starting_position"),
                                   tempExpression1266695_Udf().getField("tot_str_length")
                  )
                )
              ).otherwise(lit(null))
            )
          )
          .as("product_id"),
        element_at(tempExpression1458054_Udf(), lit(1)).as("hm_dlvry_only_ind"),
        element_at(tempExpression1458054_Udf(), lit(2)).as("hm_dlvry_stat_ind"),
        element_at(tempExpression1458054_Udf(), lit(3)).as("rebates_only_ind"),
        element_at(tempExpression1458054_Udf(), lit(4)).as("rebates_stat_ind"),
        col("dxf_src_sys_id")
          .cast(IntegerType)
          .cast(DecimalType(38, 0))
          .cast(StringType)
          .cast(DecimalType(38, 0))
          .cast(StringType)
          .as("src_env_sk"),
        when(
          when(
            isnull(callUDF("bob_180_cag_list_lkp", trim(col("acaacd")), trim(col("acaccd"))).getField("service_type")),
            lit("UNKNOWN")
          ).otherwise(
            callUDF("bob_180_cag_list_lkp", trim(col("acaacd")), trim(col("acaccd"))).getField("service_type")
          ) === lit("UNKNOWN"),
          lit("NOT IDENTIFIED")
        ).otherwise(callUDF("bob_180_cag_list_lkp", trim(col("acaacd")), trim(col("acaccd"))).getField("service_type"))
          .as("bseg_srv_typ"),
        when(
          when(
            isnull(callUDF("bob_180_cag_list_lkp", trim(col("acaacd")), trim(col("acaccd"))).getField("business_type")),
            lit("UNKNOWN")
          ).otherwise(
            callUDF("bob_180_cag_list_lkp", trim(col("acaacd")), trim(col("acaccd"))).getField("business_type")
          ) === lit("UNKNOWN"),
          lit("NOT IDENTIFIED")
        ).otherwise(
            callUDF("bob_180_cag_list_lkp", trim(col("acaacd")), trim(col("acaccd"))).getField("business_type")
          )
          .as("bseg_bus_typ"),
        when(
          when(
            isnull(callUDF("bob_180_cag_list_lkp", trim(col("acaacd")), trim(col("acaccd"))).getField("client_type")),
            lit("UNKNOWN")
          ).otherwise(
            callUDF("bob_180_cag_list_lkp", trim(col("acaacd")), trim(col("acaccd"))).getField("client_type")
          ) === lit("UNKNOWN"),
          lit("NOT IDENTIFIED")
        ).otherwise(callUDF("bob_180_cag_list_lkp", trim(col("acaacd")), trim(col("acaccd"))).getField("client_type"))
          .as("bseg_clt_typ"),
        callUDF(
          "rccagp",
          when(isnull(col("acaacd")).or(is_blank(col("acaacd"))), lit("-")).otherwise(col("acaacd")),
          when(isnull(col("acaccd")).or(is_blank(col("acaccd"))), lit("-")).otherwise(col("acaccd")),
          when(isnull(col("acadcd")).or(is_blank(col("acadcd"))), lit("-")).otherwise(col("acadcd"))
        ).getField("hyf4c6").as("busns_typ")
      ).as("ctrx_d_cag"),
      struct(
        lit(1).cast(StringType).as("_nk_is_available_"),
        when(isnull(col("acaacd")).or(is_blank(col("acaacd"))), lit("-")).otherwise(col("acaacd")).as("carrier_id"),
        when(isnull(col("acaccd")).or(is_blank(col("acaccd"))), lit("-")).otherwise(col("acaccd")).as("account_id")
      ).as("c_d_carr_acc"),
      struct(
        lit(1).cast(StringType).as("_nk_is_available_"),
        when(isnull(col("acaacd")).or(is_blank(col("acaacd"))), lit("-")).otherwise(col("acaacd")).as("carrier_id")
      ).as("ctrx_d_carr")
    )

    out

  }

  def tempExpression1250402_Udf() =
    callUDF(
      "lkp_rcagfp_orx_d_cag_custom",
      trim(col("acaacd")),
      trim(col("acaccd")),
      trim(col("acadcd")),
      lit("Specialty Indicator"),
      col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1266972_Udf() =
    callUDF(
      "d_cag_ind_lkp",
      when(isnull(col("acaacd")).or(is_blank(col("acaacd"))), lit("-")).otherwise(col("acaacd")),
      when(isnull(col("acaccd")).or(is_blank(col("acaccd"))), lit("-")).otherwise(col("acaccd")),
      when(isnull(col("acadcd")).or(is_blank(col("acadcd"))), lit("-")).otherwise(col("acadcd"))
    )

  def tempExpression1477174_Udf() =
    length(
      when(!isnull(tempExpression1251147_Udf().getField("tot_str_length")), tempExpression1430419_Udf())
        .otherwise(tempExpression1455045_Udf())
        .cast(StringType)
    ).isin(6, 7)

  def tempExpression1512230_Udf() =
    when(!isnull(tempExpression1237990_Udf().getField("tot_str_length")),   tempExpression1427163_Udf()).otherwise(
      when(!isnull(tempExpression1238235_Udf().getField("tot_str_length")), tempExpression1427213_Udf())
        .otherwise(tempExpression1494628_Udf())
    )

  def tempExpression1167742_Udf() =
    callUDF(
      "lkp_rcagfp_orx_d_cag_custom",
      trim(col("acaacd")),
      trim(col("acaccd")),
      trim(col("acadcd")),
      lit("FUNDINGARR/INDICATR"),
      col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1414059_Udf() =
    when(
      isnull(
        string_substring(tempExpression1375733_Udf(),
                         tempExpression1181421_Udf().getField("starting_position"),
                         tempExpression1181421_Udf().getField("tot_str_length")
        )
      ),
      lit("-")
    ).otherwise(
      string_substring(tempExpression1375733_Udf(),
                       tempExpression1181421_Udf().getField("starting_position"),
                       tempExpression1181421_Udf().getField("tot_str_length")
      )
    )

  def tempExpression1424549_Udf() =
    when(
      isnull(
        string_substring(tempExpression1375733_Udf(),
                         tempExpression1225920_Udf().getField("starting_position"),
                         tempExpression1225920_Udf().getField("tot_str_length")
        )
      ),
      lit("-")
    ).otherwise(
      string_substring(tempExpression1375733_Udf(),
                       tempExpression1225920_Udf().getField("starting_position"),
                       tempExpression1225920_Udf().getField("tot_str_length")
      )
    )

  def tempExpression1508408_Udf() =
    when(
      length(
        trim((lit(19000000) + decimal_strip(tempExpression1490296_Udf().cast(StringType))).cast(StringType))
      ) === lit(8),
      date_format(
        to_date(trim((lit(19000000) + decimal_strip(tempExpression1490296_Udf().cast(StringType))).cast(StringType))
                  .cast(StringType)
                  .cast(StringType),
                "yyyyMMdd"
        ),
        "yyyyMMdd"
      )
    )

  def tempExpression1250153_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            lit("*ALL"),
            lit("*ALL"),
            lit("OBLIGOR ID"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1472256_Udf() =
    length(
      when(!isnull(tempExpression1211361_Udf().getField("tot_str_length")), tempExpression1421024_Udf())
        .otherwise(tempExpression1449284_Udf())
        .cast(StringType)
    ) === lit(8)

  def tempExpression1327843_Udf() =
    when(
      !isnull(
        when(isnull(tempExpression1266972_Udf().getField("rebates_only_ind")), lit("N"))
          .otherwise(tempExpression1266972_Udf().getField("rebates_only_ind"))
      ),
      when(isnull(tempExpression1266972_Udf().getField("rebates_only_ind")), lit("N"))
        .otherwise(tempExpression1266972_Udf().getField("rebates_only_ind"))
    )

  def tempExpression1420656_Udf() =
    when(
      isnull(
        string_substring(tempExpression1375733_Udf(),
                         tempExpression1209619_Udf().getField("starting_position"),
                         tempExpression1209619_Udf().getField("tot_str_length")
        )
      ),
      lit("-")
    ).otherwise(
      string_substring(tempExpression1375733_Udf(),
                       tempExpression1209619_Udf().getField("starting_position"),
                       tempExpression1209619_Udf().getField("tot_str_length")
      )
    )

  def tempExpression1265451_Udf() =
    callUDF(
      "lkp_rcagfp_orx_d_cag_custom",
      trim(col("acaacd")),
      trim(col("acaccd")),
      trim(col("acadcd")),
      lit("GL CUSTOMER CODE"),
      col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression2293651_Udf() =
    when(
      !isnull(tempExpression1167467_Udf().getField("tot_str_length")),
      when(tempExpression1167467_Udf().getField("tot_str_length") > lit(2), lit("-")).otherwise(
        when(
          isnull(
            string_substring(tempExpression1375733_Udf(),
                             tempExpression1167467_Udf().getField("starting_position"),
                             tempExpression1167467_Udf().getField("tot_str_length")
            )
          ),
          lit("-")
        ).otherwise(
          string_substring(tempExpression1375733_Udf(),
                           tempExpression1167467_Udf().getField("starting_position"),
                           tempExpression1167467_Udf().getField("tot_str_length")
          )
        )
      )
    ).otherwise(
      when(
        !isnull(tempExpression1167742_Udf().getField("tot_str_length")),
        when(tempExpression1167742_Udf().getField("tot_str_length") > lit(2), lit("-")).otherwise(
          when(
            isnull(
              string_substring(tempExpression1375733_Udf(),
                               tempExpression1167742_Udf().getField("starting_position"),
                               tempExpression1167742_Udf().getField("tot_str_length")
              )
            ),
            lit("-")
          ).otherwise(
            string_substring(tempExpression1375733_Udf(),
                             tempExpression1167742_Udf().getField("starting_position"),
                             tempExpression1167742_Udf().getField("tot_str_length")
            )
          )
        )
      ).otherwise(
        when(
          !isnull(tempExpression1168015_Udf().getField("tot_str_length")),
          when(tempExpression1168015_Udf().getField("tot_str_length") > lit(2), lit("-")).otherwise(
            when(
              isnull(
                string_substring(tempExpression1375733_Udf(),
                                 tempExpression1168015_Udf().getField("starting_position"),
                                 tempExpression1168015_Udf().getField("tot_str_length")
                )
              ),
              lit("-")
            ).otherwise(
              string_substring(tempExpression1375733_Udf(),
                               tempExpression1168015_Udf().getField("starting_position"),
                               tempExpression1168015_Udf().getField("tot_str_length")
              )
            )
          )
        ).otherwise(tempExpression2279272_Udf())
      )
    )

  def tempExpression1472432_Udf() =
    date_format(
      to_date(
        trim(
          when(!isnull(tempExpression1211361_Udf().getField("tot_str_length")), tempExpression1421024_Udf())
            .otherwise(tempExpression1449284_Udf())
            .cast(StringType)
            .cast(StringType)
        ).cast(StringType).cast(StringType),
        "yyyyMMdd"
      ),
      "yyyyMMdd"
    )

  def tempExpression1180430_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            lit("*ALL"),
            lit("*ALL"),
            lit("ACCUM INDICATOR"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1170502_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            lit("*ALL"),
            trim(col("acadcd")),
            lit("REPORTING CODE"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1168539_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            lit("*ALL"),
            lit("*ALL"),
            lit("FUNDING ARR/INDICATR"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1163939_Udf() =
    string_substring(callUDF("client_id_cag_mask", trim(col("acaacd")), lit("*"), lit("*")).getField("client_id"),
                     lit(0),
                     lit(50)
    )

  def tempExpression1208129_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            lit("*ALL"),
            lit("*ALL"),
            lit("LEAD PARTNER"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1179685_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            lit("*ALL"),
            lit("*ALL"),
            lit("SALES OFFICE"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1430519_Udf() =
    when(
      isnull(
        string_substring(tempExpression1375733_Udf(),
                         tempExpression1251646_Udf().getField("starting_position"),
                         tempExpression1251646_Udf().getField("tot_str_length")
        )
      ),
      lit("-")
    ).otherwise(
      string_substring(tempExpression1375733_Udf(),
                       tempExpression1251646_Udf().getField("starting_position"),
                       tempExpression1251646_Udf().getField("tot_str_length")
      )
    )

  def tempExpression1249654_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            trim(col("acaccd")),
            trim(col("acadcd")),
            lit("OBLIGOR ID"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1175216_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            lit("*ALL"),
            lit("*ALL"),
            lit("SOLD MARKE SITE"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1472050_Udf() =
    length(
      when(!isnull(tempExpression1211361_Udf().getField("tot_str_length")), tempExpression1421024_Udf())
        .otherwise(tempExpression1449284_Udf())
        .cast(StringType)
    ).isin(6, 7)

  def tempExpression1451607_Udf() =
    when(!isnull(tempExpression1225920_Udf().getField("tot_str_length")),   tempExpression1424549_Udf()).otherwise(
      when(!isnull(tempExpression1226165_Udf().getField("tot_str_length")), tempExpression1424599_Udf())
        .otherwise(lit(null).cast(StringType))
    )

  def tempExpression1223685_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            lit("*ALL"),
            trim(col("acadcd")),
            lit("PRODUCT CLASS ID"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1168015_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            lit("*ALL"),
            trim(col("acadcd")),
            lit("FUNDING ARR/INDICATR"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1263217_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            trim(col("acaccd")),
            trim(col("acadcd")),
            lit("Pick LOB"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1179931_Udf() =
    callUDF(
      "lkp_rcagfp_orx_d_cag_custom",
      trim(col("acaacd")),
      trim(col("acaccd")),
      trim(col("acadcd")),
      lit("ACCUM INDICATOR"),
      col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1266695_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            lit("*ALL"),
            lit("*ALL"),
            lit("PRODUCT ID"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1174717_Udf() =
    callUDF(
      "lkp_rcagfp_orx_d_cag_custom",
      trim(col("acaacd")),
      trim(col("acaccd")),
      trim(col("acadcd")),
      lit("SOLD MARKE SITE"),
      col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1164489_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            trim(col("acaccd")),
            trim(col("acadcd")),
            lit("CLAIM SUFFIX"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1468622_Udf() =
    when(!isnull(tempExpression1181920_Udf().getField("tot_str_length")),   tempExpression1414159_Udf()).otherwise(
      when(!isnull(tempExpression1182162_Udf().getField("tot_str_length")), tempExpression1414209_Udf())
        .otherwise(tempExpression1445283_Udf())
    )

  def tempExpression1427263_Udf() =
    when(
      isnull(
        string_substring(tempExpression1375733_Udf(),
                         tempExpression1238477_Udf().getField("starting_position"),
                         tempExpression1238477_Udf().getField("tot_str_length")
        )
      ),
      lit("-")
    ).otherwise(
      string_substring(tempExpression1375733_Udf(),
                       tempExpression1238477_Udf().getField("starting_position"),
                       tempExpression1238477_Udf().getField("tot_str_length")
      )
    )

  def tempExpression2278243_Udf() =
    when(
      !isnull(tempExpression1164988_Udf().getField("tot_str_length")),
      when(
        isnull(
          string_substring(tempExpression1375733_Udf(),
                           tempExpression1164988_Udf().getField("starting_position"),
                           tempExpression1164988_Udf().getField("tot_str_length")
          )
        ),
        lit("-")
      ).otherwise(
        string_substring(tempExpression1375733_Udf(),
                         tempExpression1164988_Udf().getField("starting_position"),
                         tempExpression1164988_Udf().getField("tot_str_length")
        )
      )
    ).otherwise(
      when(
        !isnull(tempExpression1165230_Udf().getField("tot_str_length")),
        when(
          isnull(
            string_substring(tempExpression1375733_Udf(),
                             tempExpression1165230_Udf().getField("starting_position"),
                             tempExpression1165230_Udf().getField("tot_str_length")
            )
          ),
          lit("-")
        ).otherwise(
          string_substring(tempExpression1375733_Udf(),
                           tempExpression1165230_Udf().getField("starting_position"),
                           tempExpression1165230_Udf().getField("tot_str_length")
          )
        )
      ).otherwise(
        when(
          !isnull(tempExpression1165484_Udf().getField("tot_str_length")),
          when(
            isnull(
              string_substring(tempExpression1375733_Udf(),
                               tempExpression1165484_Udf().getField("starting_position"),
                               tempExpression1165484_Udf().getField("tot_str_length")
              )
            ),
            lit("-")
          ).otherwise(
            string_substring(tempExpression1375733_Udf(),
                             tempExpression1165484_Udf().getField("starting_position"),
                             tempExpression1165484_Udf().getField("tot_str_length")
            )
          )
        ).otherwise(
          when(
            !isnull(tempExpression1165729_Udf().getField("tot_str_length")),
            when(
              isnull(
                string_substring(tempExpression1375733_Udf(),
                                 tempExpression1165729_Udf().getField("starting_position"),
                                 tempExpression1165729_Udf().getField("tot_str_length")
                )
              ),
              lit("-")
            ).otherwise(
              string_substring(tempExpression1375733_Udf(),
                               tempExpression1165729_Udf().getField("starting_position"),
                               tempExpression1165729_Udf().getField("tot_str_length")
              )
            )
          ).otherwise(lit("-"))
        )
      )
    )

  def tempExpression1403201_Udf() =
    (!isnull(tempExpression1371628_Udf())).and(size(tempExpression1371628_Udf()) === lit(0))

  def tempExpression1206639_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            lit("*ALL"),
            lit("*ALL"),
            lit("LEGAL ENTITY"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1181920_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            lit("*ALL"),
            lit("*ALL"),
            lit("RENEWAL DATE"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1178940_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            lit("*ALL"),
            lit("*ALL"),
            lit("SA INDICATOR"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1371628_Udf() =
    when(
      !isnull(tempExpression1266972_Udf().getField("carrier_id")),
      array(tempExpression1327763_Udf(),
            tempExpression1327803_Udf(),
            tempExpression1327843_Udf(),
            tempExpression1327883_Udf()
      )
    ).otherwise(array())

  def tempExpression1173726_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            lit("*ALL"),
            lit("*ALL"),
            lit("HMO ACCOUNT DIV"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression2279272_Udf() =
    when(
      !isnull(tempExpression1168278_Udf().getField("tot_str_length")),
      when(tempExpression1168278_Udf().getField("tot_str_length") > lit(2), lit("-")).otherwise(
        when(
          isnull(
            string_substring(tempExpression1375733_Udf(),
                             tempExpression1168278_Udf().getField("starting_position"),
                             tempExpression1168278_Udf().getField("tot_str_length")
            )
          ),
          lit("-")
        ).otherwise(
          string_substring(tempExpression1375733_Udf(),
                           tempExpression1168278_Udf().getField("starting_position"),
                           tempExpression1168278_Udf().getField("tot_str_length")
          )
        )
      )
    ).otherwise(
      when(
        !isnull(tempExpression1168539_Udf().getField("tot_str_length")),
        when(tempExpression1168539_Udf().getField("tot_str_length") > lit(2), lit("-")).otherwise(
          when(
            isnull(
              string_substring(tempExpression1375733_Udf(),
                               tempExpression1168539_Udf().getField("starting_position"),
                               tempExpression1168539_Udf().getField("tot_str_length")
              )
            ),
            lit("-")
          ).otherwise(
            string_substring(tempExpression1375733_Udf(),
                             tempExpression1168539_Udf().getField("starting_position"),
                             tempExpression1168539_Udf().getField("tot_str_length")
            )
          )
        )
      ).otherwise(
        when(
          !isnull(tempExpression1168790_Udf().getField("tot_str_length")),
          when(tempExpression1168790_Udf().getField("tot_str_length") > lit(2), lit("-")).otherwise(
            when(
              isnull(
                string_substring(tempExpression1375733_Udf(),
                                 tempExpression1168790_Udf().getField("starting_position"),
                                 tempExpression1168790_Udf().getField("tot_str_length")
                )
              ),
              lit("-")
            ).otherwise(
              string_substring(tempExpression1375733_Udf(),
                               tempExpression1168790_Udf().getField("starting_position"),
                               tempExpression1168790_Udf().getField("tot_str_length")
              )
            )
          )
        ).otherwise(lit("-"))
      )
    )

  def tempExpression1172479_Udf() =
    callUDF(
      "lkp_rcagfp_orx_d_cag_custom",
      trim(col("acaacd")),
      trim(col("acaccd")),
      trim(col("acadcd")),
      lit("SEGMENT INDICATOR"),
      col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1164620_Udf() =
    string_substring(
      callUDF("lkp_rcagdp", col("acaacd"), col("acaccd"), lit("*ALL"), col("dxf_src_sys_id").cast(IntegerType))
        .getField("h7ant4"),
      lit(2),
      length(
        callUDF("lkp_rcagdp", col("acaacd"), col("acaccd"), lit("*ALL"), col("dxf_src_sys_id").cast(IntegerType))
          .getField("h7ant4")
      )
    )

  def tempExpression1403117_Udf() =
    when(
      !isnull(
        when(isnull(tempExpression1266972_Udf().getField("rebates_only_ind")), lit("N"))
          .otherwise(tempExpression1266972_Udf().getField("rebates_only_ind"))
      ),
      when(!lit(0).cast(BooleanType), lit(1).cast(BooleanType)).otherwise(tempExpression1371418_Udf())
    ).otherwise(tempExpression1371418_Udf())

  def tempExpression1224430_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            lit("*ALL"),
            trim(col("acadcd")),
            lit("PHARMACY RIDER"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1168790_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            lit("*ALL"),
            lit("*ALL"),
            lit("FUNDINGARR/INDICATR"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1455045_Udf() =
    when(!isnull(tempExpression1251401_Udf().getField("tot_str_length")),   tempExpression1430469_Udf()).otherwise(
      when(!isnull(tempExpression1251646_Udf().getField("tot_str_length")), tempExpression1430519_Udf())
        .otherwise(lit(null).cast(StringType))
    )

  def tempExpression1427213_Udf() =
    when(
      isnull(
        string_substring(tempExpression1375733_Udf(),
                         tempExpression1238235_Udf().getField("starting_position"),
                         tempExpression1238235_Udf().getField("tot_str_length")
        )
      ),
      lit("-")
    ).otherwise(
      string_substring(tempExpression1375733_Udf(),
                       tempExpression1238235_Udf().getField("starting_position"),
                       tempExpression1238235_Udf().getField("tot_str_length")
      )
    )

  def tempExpression1209619_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            lit("*ALL"),
            lit("*ALL"),
            lit("MIGRATION INDICATOR"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1176951_Udf() =
    callUDF(
      "lkp_rcagfp_orx_d_cag_custom",
      trim(col("acaacd")),
      trim(col("acaccd")),
      trim(col("acadcd")),
      lit("PLATFORM INDICATOR"),
      col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1177450_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            lit("*ALL"),
            lit("*ALL"),
            lit("PLATFORM INDICATOR"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1250901_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            lit("*ALL"),
            lit("*ALL"),
            lit("Specialty Indicator"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1163839_Udf() =
    callUDF(
      "client_id_cag_mask",
      trim(col("acaacd")),
      when(isnull(trim(col("acaccd"))), lit("*")).otherwise(trim(col("acaccd"))),
      when(isnull(trim(col("acadcd"))), lit("*")).otherwise(trim(col("acadcd")))
    )

  def tempExpression1181675_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            lit("*ALL"),
            trim(col("acadcd")),
            lit("RENEWAL DATE"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1427513_Udf() =
    when(
      isnull(
        string_substring(tempExpression1375733_Udf(),
                         tempExpression1239717_Udf().getField("starting_position"),
                         tempExpression1239717_Udf().getField("tot_str_length")
        )
      ),
      lit("-")
    ).otherwise(
      string_substring(tempExpression1375733_Udf(),
                       tempExpression1239717_Udf().getField("starting_position"),
                       tempExpression1239717_Udf().getField("tot_str_length")
      )
    )

  def tempExpression1177205_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            lit("*ALL"),
            trim(col("acadcd")),
            lit("PLATFORM INDICATOR"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1445283_Udf() =
    when(!isnull(tempExpression1182416_Udf().getField("tot_str_length")),   tempExpression1414259_Udf()).otherwise(
      when(!isnull(tempExpression1182661_Udf().getField("tot_str_length")), tempExpression1414309_Udf())
        .otherwise(lit(null).cast(StringType))
    )

  def tempExpression1164145_Udf() =
    callUDF("d_carrier", when(isnull(col("acaacd")).or(is_blank(col("acaacd"))), lit("-")).otherwise(col("acaacd")))

  def tempExpression1239717_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            lit("*ALL"),
            lit("*ALL"),
            lit("TRANS DATE TIMESTAMP"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1496186_Udf() =
    is_valid(
      when(
        length(trim(tempExpression1477239_Udf().cast(StringType))) === lit(8),
        date_format(
          to_date(trim(tempExpression1477239_Udf().cast(StringType)).cast(StringType).cast(StringType), "yyyyMMdd"),
          "yyyyMMdd"
        )
      )
    )

  def tempExpression1494342_Udf() =
    when(isnull(
           tempExpression1474348_Udf()
             .and(is_valid(when(tempExpression1474435_Udf(), tempExpression1474524_Udf())) === lit(1))
         ),
         lit(0).cast(BooleanType)
    ).otherwise(
      tempExpression1474348_Udf()
        .and(is_valid(when(tempExpression1474435_Udf(), tempExpression1474524_Udf())) === lit(1))
    )

  def tempExpression1477239_Udf() =
    lit(19000000) + decimal_strip(
      when(!isnull(tempExpression1251147_Udf().getField("tot_str_length")), tempExpression1430419_Udf())
        .otherwise(tempExpression1455045_Udf())
        .cast(StringType)
    )

  def tempExpression1249908_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            lit("*ALL"),
            trim(col("acadcd")),
            lit("OBLIGOR ID"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1165230_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            trim(col("acaccd")),
            trim(col("acadcd")),
            lit("Claim Suffix"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1427163_Udf() =
    when(
      isnull(
        string_substring(tempExpression1375733_Udf(),
                         tempExpression1237990_Udf().getField("starting_position"),
                         tempExpression1237990_Udf().getField("tot_str_length")
        )
      ),
      lit("-")
    ).otherwise(
      string_substring(tempExpression1375733_Udf(),
                       tempExpression1237990_Udf().getField("starting_position"),
                       tempExpression1237990_Udf().getField("tot_str_length")
      )
    )

  def tempExpression1205395_Udf() =
    callUDF(
      "lkp_rcagfp_orx_d_cag_custom",
      trim(col("acaacd")),
      trim(col("acaccd")),
      trim(col("acadcd")),
      lit("IPLAN INDICATOR"),
      col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1211361_Udf() =
    callUDF(
      "lkp_rcagfp_orx_d_cag_custom",
      trim(col("acaacd")),
      trim(col("acaccd")),
      trim(col("acadcd")),
      lit("MIGRATION EFF DATE"),
      col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1327803_Udf() =
    when(
      !isnull(
        when(isnull(tempExpression1266972_Udf().getField("hm_dlvry_stat_ind")), lit("N"))
          .otherwise(tempExpression1266972_Udf().getField("hm_dlvry_stat_ind"))
      ),
      when(isnull(tempExpression1266972_Udf().getField("hm_dlvry_stat_ind")), lit("N"))
        .otherwise(tempExpression1266972_Udf().getField("hm_dlvry_stat_ind"))
    )

  def tempExpression1263716_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            lit("*ALL"),
            lit("*ALL"),
            lit("Pick LOB"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1525847_Udf() =
    when(isnull(
           length(tempExpression1490296_Udf().cast(StringType))
             .isin(6, 7)
             .and(is_valid(tempExpression1508408_Udf()) === lit(1))
         ),
         lit(0).cast(BooleanType)
    ).otherwise(
      length(tempExpression1490296_Udf().cast(StringType))
        .isin(6, 7)
        .and(is_valid(tempExpression1508408_Udf()) === lit(1))
    )

  def tempExpression1180185_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            lit("*ALL"),
            trim(col("acadcd")),
            lit("ACCUM INDICATOR"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1225920_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            lit("*ALL"),
            trim(col("acadcd")),
            lit("COB/EFF DATE"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1238731_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            lit("*ALL"),
            trim(col("acadcd")),
            lit("TRANS TIMESTAMP"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1250656_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            lit("*ALL"),
            trim(col("acadcd")),
            lit("Specialty Indicator"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1375261_Udf() =
    when(!isnull(tempExpression1163839_Udf().getField("client_id")),
         string_substring(tempExpression1163839_Udf().getField("client_id"), lit(0), lit(50))
    ).otherwise(tempExpression1334538_Udf())

  def tempExpression1420606_Udf() =
    when(
      isnull(
        string_substring(tempExpression1375733_Udf(),
                         tempExpression1209374_Udf().getField("starting_position"),
                         tempExpression1209374_Udf().getField("tot_str_length")
        )
      ),
      lit("-")
    ).otherwise(
      string_substring(tempExpression1375733_Udf(),
                       tempExpression1209374_Udf().getField("starting_position"),
                       tempExpression1209374_Udf().getField("tot_str_length")
      )
    )

  def tempExpression1166973_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            lit("*ALL"),
            trim(col("acadcd")),
            lit("CUSTOMER NUMBER"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression2278695_Udf() =
    when(
      !isnull(tempExpression1166477_Udf().getField("tot_str_length")),
      when(
        isnull(
          string_substring(tempExpression1375733_Udf(),
                           tempExpression1166477_Udf().getField("starting_position"),
                           tempExpression1166477_Udf().getField("tot_str_length")
          )
        ),
        lit("-")
      ).otherwise(
        string_substring(tempExpression1375733_Udf(),
                         tempExpression1166477_Udf().getField("starting_position"),
                         tempExpression1166477_Udf().getField("tot_str_length")
        )
      )
    ).otherwise(
      when(
        !isnull(tempExpression1166719_Udf().getField("tot_str_length")),
        when(
          isnull(
            string_substring(tempExpression1375733_Udf(),
                             tempExpression1166719_Udf().getField("starting_position"),
                             tempExpression1166719_Udf().getField("tot_str_length")
            )
          ),
          lit("-")
        ).otherwise(
          string_substring(tempExpression1375733_Udf(),
                           tempExpression1166719_Udf().getField("starting_position"),
                           tempExpression1166719_Udf().getField("tot_str_length")
          )
        )
      ).otherwise(
        when(
          !isnull(tempExpression1166973_Udf().getField("tot_str_length")),
          when(
            isnull(
              string_substring(tempExpression1375733_Udf(),
                               tempExpression1166973_Udf().getField("starting_position"),
                               tempExpression1166973_Udf().getField("tot_str_length")
              )
            ),
            lit("-")
          ).otherwise(
            string_substring(tempExpression1375733_Udf(),
                             tempExpression1166973_Udf().getField("starting_position"),
                             tempExpression1166973_Udf().getField("tot_str_length")
            )
          )
        ).otherwise(
          when(
            !isnull(tempExpression1167218_Udf().getField("tot_str_length")),
            when(
              isnull(
                string_substring(tempExpression1375733_Udf(),
                                 tempExpression1167218_Udf().getField("starting_position"),
                                 tempExpression1167218_Udf().getField("tot_str_length")
                )
              ),
              lit("-")
            ).otherwise(
              string_substring(tempExpression1375733_Udf(),
                               tempExpression1167218_Udf().getField("starting_position"),
                               tempExpression1167218_Udf().getField("tot_str_length")
              )
            )
          ).otherwise(lit("-"))
        )
      )
    )

  def tempExpression1172237_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            lit("*ALL"),
            lit("*ALL"),
            lit("SEGEMENT INDICATOR"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1224176_Udf() =
    callUDF(
      "lkp_rcagfp_orx_d_cag_custom",
      trim(col("acaacd")),
      trim(col("acaccd")),
      trim(col("acadcd")),
      lit("PHARMACY RIDER"),
      col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1421124_Udf() =
    when(
      isnull(
        string_substring(tempExpression1375733_Udf(),
                         tempExpression1211860_Udf().getField("starting_position"),
                         tempExpression1211860_Udf().getField("tot_str_length")
        )
      ),
      lit("-")
    ).otherwise(
      string_substring(tempExpression1375733_Udf(),
                       tempExpression1211860_Udf().getField("starting_position"),
                       tempExpression1211860_Udf().getField("tot_str_length")
      )
    )

  def tempExpression1424499_Udf() =
    when(
      isnull(
        string_substring(tempExpression1375733_Udf(),
                         tempExpression1225666_Udf().getField("starting_position"),
                         tempExpression1225666_Udf().getField("tot_str_length")
        )
      ),
      lit("-")
    ).otherwise(
      string_substring(tempExpression1375733_Udf(),
                       tempExpression1225666_Udf().getField("starting_position"),
                       tempExpression1225666_Udf().getField("tot_str_length")
      )
    )

  def tempExpression1335055_Udf() =
    when(
      !isnull(
        callUDF("lkp_rcagdp", col("acaacd"), col("acaccd"), lit("*ALL"), col("dxf_src_sys_id").cast(IntegerType))
          .getField("h7ant4")
      ),
      tempExpression1164620_Udf()
    ).otherwise(tempExpression1282730_Udf())

  def tempExpression2288393_Udf() =
    when(
      !isnull(tempExpression1249412_Udf().getField("tot_str_length")),
      when(
        isnull(
          string_substring(tempExpression1375733_Udf(),
                           tempExpression1249412_Udf().getField("starting_position"),
                           tempExpression1249412_Udf().getField("tot_str_length")
          )
        ),
        lit("-")
      ).otherwise(
        string_substring(tempExpression1375733_Udf(),
                         tempExpression1249412_Udf().getField("starting_position"),
                         tempExpression1249412_Udf().getField("tot_str_length")
        )
      )
    ).otherwise(
      when(
        !isnull(tempExpression1249654_Udf().getField("tot_str_length")),
        when(
          isnull(
            string_substring(tempExpression1375733_Udf(),
                             tempExpression1249654_Udf().getField("starting_position"),
                             tempExpression1249654_Udf().getField("tot_str_length")
            )
          ),
          lit("-")
        ).otherwise(
          string_substring(tempExpression1375733_Udf(),
                           tempExpression1249654_Udf().getField("starting_position"),
                           tempExpression1249654_Udf().getField("tot_str_length")
          )
        )
      ).otherwise(
        when(
          !isnull(tempExpression1249908_Udf().getField("tot_str_length")),
          when(
            isnull(
              string_substring(tempExpression1375733_Udf(),
                               tempExpression1249908_Udf().getField("starting_position"),
                               tempExpression1249908_Udf().getField("tot_str_length")
              )
            ),
            lit("-")
          ).otherwise(
            string_substring(tempExpression1375733_Udf(),
                             tempExpression1249908_Udf().getField("starting_position"),
                             tempExpression1249908_Udf().getField("tot_str_length")
            )
          )
        ).otherwise(
          when(
            !isnull(tempExpression1250153_Udf().getField("tot_str_length")),
            when(
              isnull(
                string_substring(tempExpression1375733_Udf(),
                                 tempExpression1250153_Udf().getField("starting_position"),
                                 tempExpression1250153_Udf().getField("tot_str_length")
                )
              ),
              lit("-")
            ).otherwise(
              string_substring(tempExpression1375733_Udf(),
                               tempExpression1250153_Udf().getField("starting_position"),
                               tempExpression1250153_Udf().getField("tot_str_length")
              )
            )
          ).otherwise(lit(null))
        )
      )
    )

  def tempExpression1472115_Udf() =
    lit(19000000) + decimal_strip(
      when(!isnull(tempExpression1211361_Udf().getField("tot_str_length")), tempExpression1421024_Udf())
        .otherwise(tempExpression1449284_Udf())
        .cast(StringType)
    )

  def tempExpression1208375_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            trim(col("acaccd")),
            trim(col("acadcd")),
            lit("COC CODE"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1172978_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            lit("*ALL"),
            lit("*ALL"),
            lit("SEGMENT INDICATOR"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1414309_Udf() =
    when(
      isnull(
        string_substring(tempExpression1375733_Udf(),
                         tempExpression1182661_Udf().getField("starting_position"),
                         tempExpression1182661_Udf().getField("tot_str_length")
        )
      ),
      lit("-")
    ).otherwise(
      string_substring(tempExpression1375733_Udf(),
                       tempExpression1182661_Udf().getField("starting_position"),
                       tempExpression1182661_Udf().getField("tot_str_length")
      )
    )

  def tempExpression1427113_Udf() =
    when(
      isnull(
        string_substring(tempExpression1375733_Udf(),
                         tempExpression1237736_Udf().getField("starting_position"),
                         tempExpression1237736_Udf().getField("tot_str_length")
        )
      ),
      lit("-")
    ).otherwise(
      string_substring(tempExpression1375733_Udf(),
                       tempExpression1237736_Udf().getField("starting_position"),
                       tempExpression1237736_Udf().getField("tot_str_length")
      )
    )

  def tempExpression1449035_Udf() =
    when(!isnull(tempExpression1209374_Udf().getField("tot_str_length")),   tempExpression1420606_Udf()).otherwise(
      when(!isnull(tempExpression1209619_Udf().getField("tot_str_length")), tempExpression1420656_Udf())
        .otherwise(lit(null))
    )

  def tempExpression1171738_Udf() =
    callUDF(
      "lkp_rcagfp_orx_d_cag_custom",
      trim(col("acaacd")),
      trim(col("acaccd")),
      trim(col("acadcd")),
      lit("SEGEMENT INDICATOR"),
      col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1170248_Udf() =
    callUDF(
      "lkp_rcagfp_orx_d_cag_custom",
      trim(col("acaacd")),
      trim(col("acaccd")),
      trim(col("acadcd")),
      lit("REPORTING CODE"),
      col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1168278_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            lit("*ALL"),
            trim(col("acadcd")),
            lit("FUNDINGARR/INDICATR"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1207139_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            lit("*ALL"),
            trim(col("acadcd")),
            lit("FRANCHISE NUMBER"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1171992_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            lit("*ALL"),
            trim(col("acadcd")),
            lit("SEGEMENT INDICATOR"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1474435_Udf() =
    length(
      trim(
        when(!isnull(tempExpression1225666_Udf().getField("tot_str_length")), tempExpression1424499_Udf())
          .otherwise(tempExpression1451607_Udf())
          .cast(StringType)
          .cast(StringType)
      )
    ) === lit(8)

  def tempExpression1475460_Udf() =
    when(!isnull(tempExpression1238976_Udf().getField("tot_str_length")),   tempExpression1427363_Udf()).otherwise(
      when(!isnull(tempExpression1239218_Udf().getField("tot_str_length")), tempExpression1427413_Udf())
        .otherwise(tempExpression1453174_Udf())
    )

  def tempExpression1430469_Udf() =
    when(
      isnull(
        string_substring(tempExpression1375733_Udf(),
                         tempExpression1251401_Udf().getField("starting_position"),
                         tempExpression1251401_Udf().getField("tot_str_length")
        )
      ),
      lit("-")
    ).otherwise(
      string_substring(tempExpression1375733_Udf(),
                       tempExpression1251401_Udf().getField("starting_position"),
                       tempExpression1251401_Udf().getField("tot_str_length")
      )
    )

  def tempExpression1206885_Udf() =
    callUDF(
      "lkp_rcagfp_orx_d_cag_custom",
      trim(col("acaacd")),
      trim(col("acaccd")),
      trim(col("acadcd")),
      lit("FRANCHISE NUMBER"),
      col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1178695_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            lit("*ALL"),
            trim(col("acadcd")),
            lit("SA INDICATOR"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1265950_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            lit("*ALL"),
            lit("*ALL"),
            lit("GL CUSTOMER CODE"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1449284_Udf() =
    when(!isnull(tempExpression1211615_Udf().getField("tot_str_length")),   tempExpression1421074_Udf()).otherwise(
      when(!isnull(tempExpression1211860_Udf().getField("tot_str_length")), tempExpression1421124_Udf())
        .otherwise(lit(null).cast(StringType))
    )

  def tempExpression1334538_Udf() =
    when(!isnull(tempExpression1163880_Udf().getField("client_id")),
         string_substring(tempExpression1163880_Udf().getField("client_id"), lit(0), lit(50))
    ).otherwise(tempExpression1282177_Udf())

  def tempExpression1209120_Udf() =
    callUDF(
      "lkp_rcagfp_orx_d_cag_custom",
      trim(col("acaacd")),
      trim(col("acaccd")),
      trim(col("acadcd")),
      lit("MIGRATION INDICATOR"),
      col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1420556_Udf() =
    when(
      isnull(
        string_substring(tempExpression1375733_Udf(),
                         tempExpression1209120_Udf().getField("starting_position"),
                         tempExpression1209120_Udf().getField("tot_str_length")
        )
      ),
      lit("-")
    ).otherwise(
      string_substring(tempExpression1375733_Udf(),
                       tempExpression1209120_Udf().getField("starting_position"),
                       tempExpression1209120_Udf().getField("tot_str_length")
      )
    )

  def tempExpression1265705_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            lit("*ALL"),
            trim(col("acadcd")),
            lit("GL CUSTOMER CODE"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1207384_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            lit("*ALL"),
            lit("*ALL"),
            lit("FRANCHISE NUMBER"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1249412_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            lit("*ALL"),
            lit("*ALL"),
            lit("OBLIGOR CODE"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1421074_Udf() =
    when(
      isnull(
        string_substring(tempExpression1375733_Udf(),
                         tempExpression1211615_Udf().getField("starting_position"),
                         tempExpression1211615_Udf().getField("tot_str_length")
        )
      ),
      lit("-")
    ).otherwise(
      string_substring(tempExpression1375733_Udf(),
                       tempExpression1211615_Udf().getField("starting_position"),
                       tempExpression1211615_Udf().getField("tot_str_length")
      )
    )

  def tempExpression1248913_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            trim(col("acaccd")),
            trim(col("acadcd")),
            lit("OBLIGOR CODE"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1179440_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            lit("*ALL"),
            trim(col("acadcd")),
            lit("SALES OFFICE"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1225666_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            trim(col("acaccd")),
            trim(col("acadcd")),
            lit("COB/EFF DATE"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1173972_Udf() =
    callUDF(
      "lkp_rcagfp_orx_d_cag_custom",
      trim(col("acaacd")),
      trim(col("acaccd")),
      trim(col("acadcd")),
      lit("CLAIM ACCOUNT CODE"),
      col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1174226_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            lit("*ALL"),
            trim(col("acadcd")),
            lit("CLAIM ACCOUNT CODE"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1166719_Udf() =
    callUDF(
      "lkp_rcagfp_orx_d_cag_custom",
      trim(col("acaacd")),
      trim(col("acaccd")),
      trim(col("acadcd")),
      lit("CUSTOMER NUMBER"),
      col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1238477_Udf() =
    callUDF(
      "lkp_rcagfp_orx_d_cag_custom",
      trim(col("acaacd")),
      trim(col("acaccd")),
      trim(col("acadcd")),
      lit("TRANS TIMESTAMP"),
      col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1211860_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            lit("*ALL"),
            lit("*ALL"),
            lit("MIGRATION EFF DATE"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1430419_Udf() =
    when(
      isnull(
        string_substring(tempExpression1375733_Udf(),
                         tempExpression1251147_Udf().getField("starting_position"),
                         tempExpression1251147_Udf().getField("tot_str_length")
        )
      ),
      lit("-")
    ).otherwise(
      string_substring(tempExpression1375733_Udf(),
                       tempExpression1251147_Udf().getField("starting_position"),
                       tempExpression1251147_Udf().getField("tot_str_length")
      )
    )

  def tempExpression1167218_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            lit("*ALL"),
            lit("*ALL"),
            lit("CUSTOMER NUMBER"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1164988_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            lit("*ALL"),
            lit("*ALL"),
            lit("CLAIM SUFFIX"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1458054_Udf() =
    when(
      !when(!isnull(tempExpression1266972_Udf().getField("carrier_id")), tempExpression1434605_Udf())
        .otherwise(lit(0).cast(BooleanType))
        .cast(BooleanType),
      tempExpression1434629_Udf()
    ).otherwise(tempExpression1371628_Udf())

  def tempExpression1207630_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            trim(col("acaccd")),
            trim(col("acadcd")),
            lit("LEAD PARTNER"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1211615_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            lit("*ALL"),
            trim(col("acadcd")),
            lit("MIGRATION EFF DATE"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1226165_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            lit("*ALL"),
            lit("*ALL"),
            lit("COB/EFF DATE"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1174471_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            lit("*ALL"),
            lit("*ALL"),
            lit("CLAIM ACCOUNT CODE"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1414259_Udf() =
    when(
      isnull(
        string_substring(tempExpression1375733_Udf(),
                         tempExpression1182416_Udf().getField("starting_position"),
                         tempExpression1182416_Udf().getField("tot_str_length")
        )
      ),
      lit("-")
    ).otherwise(
      string_substring(tempExpression1375733_Udf(),
                       tempExpression1182416_Udf().getField("starting_position"),
                       tempExpression1182416_Udf().getField("tot_str_length")
      )
    )

  def tempExpression1167467_Udf() =
    callUDF(
      "lkp_rcagfp_orx_d_cag_custom",
      trim(col("acaacd")),
      trim(col("acaccd")),
      trim(col("acadcd")),
      lit("FUNDING ARR/INDICATR"),
      col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1206140_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            trim(col("acaccd")),
            trim(col("acadcd")),
            lit("LEGAL ENTITY"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1474348_Udf() =
    length(
      when(!isnull(tempExpression1225666_Udf().getField("tot_str_length")), tempExpression1424499_Udf())
        .otherwise(tempExpression1451607_Udf())
        .cast(StringType)
    ) === lit(8)

  def tempExpression1180930_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            lit("*ALL"),
            trim(col("acadcd")),
            lit("ERISA INDICATOR"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1251401_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            lit("*ALL"),
            trim(col("acadcd")),
            lit("Specialty Eff Date"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1427463_Udf() =
    when(
      isnull(
        string_substring(tempExpression1375733_Udf(),
                         tempExpression1239472_Udf().getField("starting_position"),
                         tempExpression1239472_Udf().getField("tot_str_length")
        )
      ),
      lit("-")
    ).otherwise(
      string_substring(tempExpression1375733_Udf(),
                       tempExpression1239472_Udf().getField("starting_position"),
                       tempExpression1239472_Udf().getField("tot_str_length")
      )
    )

  def tempExpression1490296_Udf() =
    when(!isnull(tempExpression1181421_Udf().getField("tot_str_length")),   tempExpression1414059_Udf()).otherwise(
      when(!isnull(tempExpression1181675_Udf().getField("tot_str_length")), tempExpression1414109_Udf())
        .otherwise(tempExpression1468622_Udf())
    )

  def tempExpression1208629_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            lit("*ALL"),
            trim(col("acadcd")),
            lit("COC CODE"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1249167_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            lit("*ALL"),
            trim(col("acadcd")),
            lit("OBLIGOR CODE"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1266450_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            lit("*ALL"),
            trim(col("acadcd")),
            lit("PRODUCT ID"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1282730_Udf() =
    when(
      !isnull(
        callUDF("lkp_rcagdp", col("acaacd"), lit("*ALL"), lit("*ALL"), col("dxf_src_sys_id").cast(IntegerType))
          .getField("h7ant4")
      ),
      tempExpression1164686_Udf()
    ).otherwise(lit("-"))

  def tempExpression1175712_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            lit("*ALL"),
            trim(col("acadcd")),
            lit("SOLD MARKET SITE"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1166232_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            lit("*ALL"),
            trim(col("acadcd")),
            lit("Customer Number"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1474524_Udf() =
    date_format(
      to_date(
        trim(
          when(!isnull(tempExpression1225666_Udf().getField("tot_str_length")), tempExpression1424499_Udf())
            .otherwise(tempExpression1451607_Udf())
            .cast(StringType)
            .cast(StringType)
        ).cast(StringType).cast(StringType),
        "yyyyMMdd"
      ),
      "yyyyMMdd"
    )

  def tempExpression1414209_Udf() =
    when(
      isnull(
        string_substring(tempExpression1375733_Udf(),
                         tempExpression1182162_Udf().getField("starting_position"),
                         tempExpression1182162_Udf().getField("tot_str_length")
        )
      ),
      lit("-")
    ).otherwise(
      string_substring(tempExpression1375733_Udf(),
                       tempExpression1182162_Udf().getField("starting_position"),
                       tempExpression1182162_Udf().getField("tot_str_length")
      )
    )

  def tempExpression1264212_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            lit("*ALL"),
            trim(col("acadcd")),
            lit("LOB"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1208874_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            lit("*ALL"),
            lit("*ALL"),
            lit("COC CODE"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1421024_Udf() =
    when(
      isnull(
        string_substring(tempExpression1375733_Udf(),
                         tempExpression1211361_Udf().getField("starting_position"),
                         tempExpression1211361_Udf().getField("tot_str_length")
        )
      ),
      lit("-")
    ).otherwise(
      string_substring(tempExpression1375733_Udf(),
                       tempExpression1211361_Udf().getField("starting_position"),
                       tempExpression1211361_Udf().getField("tot_str_length")
      )
    )

  def tempExpression1165484_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            lit("*ALL"),
            trim(col("acadcd")),
            lit("Claim Suffix"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1224675_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            lit("*ALL"),
            lit("*ALL"),
            lit("PHARMACY RIDER"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1477380_Udf() =
    length(
      when(!isnull(tempExpression1251147_Udf().getField("tot_str_length")), tempExpression1430419_Udf())
        .otherwise(tempExpression1455045_Udf())
        .cast(StringType)
    ) === lit(8)

  def tempExpression1170747_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            lit("*ALL"),
            lit("*ALL"),
            lit("REPORTING CODE"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1474142_Udf() =
    length(
      when(!isnull(tempExpression1225666_Udf().getField("tot_str_length")), tempExpression1424499_Udf())
        .otherwise(tempExpression1451607_Udf())
        .cast(StringType)
    ).isin(6, 7)

  def tempExpression1327259_Udf() =
    when(
      !isnull(
        when(isnull(tempExpression1266972_Udf().getField("hm_dlvry_only_ind")), lit("N"))
          .otherwise(tempExpression1266972_Udf().getField("hm_dlvry_only_ind"))
      ),
      when(!lit(0).cast(BooleanType), lit(1).cast(BooleanType)).otherwise(lit(0).cast(BooleanType))
    ).otherwise(lit(0).cast(BooleanType))

  def tempExpression1176460_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            lit("*ALL"),
            trim(col("acadcd")),
            lit("SITUS STATE"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression2289468_Udf() =
    when(
      !isnull(tempExpression1263716_Udf().getField("tot_str_length")),
      when(
        isnull(
          string_substring(tempExpression1375733_Udf(),
                           tempExpression1263716_Udf().getField("starting_position"),
                           tempExpression1263716_Udf().getField("tot_str_length")
          )
        ),
        lit("-")
      ).otherwise(
        string_substring(tempExpression1375733_Udf(),
                         tempExpression1263716_Udf().getField("starting_position"),
                         tempExpression1263716_Udf().getField("tot_str_length")
        )
      )
    ).otherwise(
      when(
        !isnull(tempExpression1263958_Udf().getField("tot_str_length")),
        when(
          isnull(
            string_substring(tempExpression1375733_Udf(),
                             tempExpression1263958_Udf().getField("starting_position"),
                             tempExpression1263958_Udf().getField("tot_str_length")
            )
          ),
          lit("-")
        ).otherwise(
          string_substring(tempExpression1375733_Udf(),
                           tempExpression1263958_Udf().getField("starting_position"),
                           tempExpression1263958_Udf().getField("tot_str_length")
          )
        )
      ).otherwise(
        when(
          !isnull(tempExpression1264212_Udf().getField("tot_str_length")),
          when(
            isnull(
              string_substring(tempExpression1375733_Udf(),
                               tempExpression1264212_Udf().getField("starting_position"),
                               tempExpression1264212_Udf().getField("tot_str_length")
              )
            ),
            lit("-")
          ).otherwise(
            string_substring(tempExpression1375733_Udf(),
                             tempExpression1264212_Udf().getField("starting_position"),
                             tempExpression1264212_Udf().getField("tot_str_length")
            )
          )
        ).otherwise(
          when(
            !isnull(tempExpression1264457_Udf().getField("tot_str_length")),
            when(
              isnull(
                string_substring(tempExpression1375733_Udf(),
                                 tempExpression1264457_Udf().getField("starting_position"),
                                 tempExpression1264457_Udf().getField("tot_str_length")
                )
              ),
              lit("-")
            ).otherwise(
              string_substring(tempExpression1375733_Udf(),
                               tempExpression1264457_Udf().getField("starting_position"),
                               tempExpression1264457_Udf().getField("tot_str_length")
              )
            )
          ).otherwise(lit(null))
        )
      )
    )

  def tempExpression1182162_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            trim(col("acaccd")),
            trim(col("acadcd")),
            lit("RENWAL DATE"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1494628_Udf() =
    when(!isnull(tempExpression1238477_Udf().getField("tot_str_length")),   tempExpression1427263_Udf()).otherwise(
      when(!isnull(tempExpression1238731_Udf().getField("tot_str_length")), tempExpression1427313_Udf())
        .otherwise(tempExpression1475460_Udf())
    )

  def tempExpression1427413_Udf() =
    when(
      isnull(
        string_substring(tempExpression1375733_Udf(),
                         tempExpression1239218_Udf().getField("starting_position"),
                         tempExpression1239218_Udf().getField("tot_str_length")
        )
      ),
      lit("-")
    ).otherwise(
      string_substring(tempExpression1375733_Udf(),
                       tempExpression1239218_Udf().getField("starting_position"),
                       tempExpression1239218_Udf().getField("tot_str_length")
      )
    )

  def tempExpression1453174_Udf() =
    when(!isnull(tempExpression1239472_Udf().getField("tot_str_length")),   tempExpression1427463_Udf()).otherwise(
      when(!isnull(tempExpression1239717_Udf().getField("tot_str_length")), tempExpression1427513_Udf())
        .otherwise(lit(null))
    )

  def tempExpression1205894_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            lit("*ALL"),
            lit("*ALL"),
            lit("IPLAN INDICATOR"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1238235_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            lit("*ALL"),
            lit("*ALL"),
            lit("TRANS DATE TIMESTAM"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1205649_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            lit("*ALL"),
            trim(col("acadcd")),
            lit("IPLAN INDICATOR"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1238976_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            lit("*ALL"),
            lit("*ALL"),
            lit("TRANS TIMESTAMP"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression2280925_Udf() =
    when(
      !isnull(tempExpression1172237_Udf().getField("tot_str_length")),
      when(
        isnull(
          string_substring(tempExpression1375733_Udf(),
                           tempExpression1172237_Udf().getField("starting_position"),
                           tempExpression1172237_Udf().getField("tot_str_length")
          )
        ),
        lit("-")
      ).otherwise(
        string_substring(tempExpression1375733_Udf(),
                         tempExpression1172237_Udf().getField("starting_position"),
                         tempExpression1172237_Udf().getField("tot_str_length")
        )
      )
    ).otherwise(
      when(
        !isnull(tempExpression1172479_Udf().getField("tot_str_length")),
        when(
          isnull(
            string_substring(tempExpression1375733_Udf(),
                             tempExpression1172479_Udf().getField("starting_position"),
                             tempExpression1172479_Udf().getField("tot_str_length")
            )
          ),
          lit("-")
        ).otherwise(
          string_substring(tempExpression1375733_Udf(),
                           tempExpression1172479_Udf().getField("starting_position"),
                           tempExpression1172479_Udf().getField("tot_str_length")
          )
        )
      ).otherwise(
        when(
          !isnull(tempExpression1172733_Udf().getField("tot_str_length")),
          when(
            isnull(
              string_substring(tempExpression1375733_Udf(),
                               tempExpression1172733_Udf().getField("starting_position"),
                               tempExpression1172733_Udf().getField("tot_str_length")
              )
            ),
            lit("-")
          ).otherwise(
            string_substring(tempExpression1375733_Udf(),
                             tempExpression1172733_Udf().getField("starting_position"),
                             tempExpression1172733_Udf().getField("tot_str_length")
            )
          )
        ).otherwise(
          when(
            !isnull(tempExpression1172978_Udf().getField("tot_str_length")),
            when(
              isnull(
                string_substring(tempExpression1375733_Udf(),
                                 tempExpression1172978_Udf().getField("starting_position"),
                                 tempExpression1172978_Udf().getField("tot_str_length")
                )
              ),
              lit("-")
            ).otherwise(
              string_substring(tempExpression1375733_Udf(),
                               tempExpression1172978_Udf().getField("starting_position"),
                               tempExpression1172978_Udf().getField("tot_str_length")
              )
            )
          ).otherwise(lit("-"))
        )
      )
    )

  def tempExpression1223431_Udf() =
    callUDF(
      "lkp_rcagfp_orx_d_cag_custom",
      trim(col("acaacd")),
      trim(col("acaccd")),
      trim(col("acadcd")),
      lit("PRODUCT CLASS ID"),
      col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1179186_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            trim(col("acaccd")),
            trim(col("acadcd")),
            lit("SALES OFFICE"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1237736_Udf() =
    callUDF(
      "lkp_rcagfp_orx_d_cag_custom",
      trim(col("acaacd")),
      trim(col("acaccd")),
      trim(col("acadcd")),
      lit("TRANS DATE TIMESTAM"),
      col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1508508_Udf() =
    is_valid(
      when(
        length(trim(tempExpression1490296_Udf().cast(StringType).cast(StringType))) === lit(8),
        date_format(
          to_date(trim(tempExpression1490296_Udf().cast(StringType).cast(StringType)).cast(StringType).cast(StringType),
                  "yyyyMMdd"
          ),
          "yyyyMMdd"
        )
      )
    )

  def tempExpression1477467_Udf() =
    length(
      trim(
        when(!isnull(tempExpression1251147_Udf().getField("tot_str_length")), tempExpression1430419_Udf())
          .otherwise(tempExpression1455045_Udf())
          .cast(StringType)
          .cast(StringType)
      )
    ) === lit(8)

  def tempExpression1434605_Udf() =
    when(
      !isnull(
        when(isnull(tempExpression1266972_Udf().getField("rebates_stat_ind")), lit("N"))
          .otherwise(tempExpression1266972_Udf().getField("rebates_stat_ind"))
      ),
      when(!lit(0).cast(BooleanType), lit(1).cast(BooleanType)).otherwise(tempExpression1403117_Udf())
    ).otherwise(tempExpression1403117_Udf())

  def tempExpression1176206_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            trim(col("acaccd")),
            trim(col("acadcd")),
            lit("SITUS STATE"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1237990_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            lit("*ALL"),
            trim(col("acadcd")),
            lit("TRANS DATE TIMESTAM"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1169052_Udf() =
    callUDF(
      "lkp_rcagfp_orx_d_cag_custom",
      trim(col("acaacd")),
      trim(col("acaccd")),
      trim(col("acadcd")),
      lit("PLAN VARIATION"),
      col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1493150_Udf() =
    is_valid(
      when(
        length(trim(tempExpression1472115_Udf().cast(StringType))) === lit(8),
        date_format(
          to_date(trim(tempExpression1472115_Udf().cast(StringType)).cast(StringType).cast(StringType), "yyyyMMdd"),
          "yyyyMMdd"
        )
      )
    )

  def tempExpression1180676_Udf() =
    callUDF(
      "lkp_rcagfp_orx_d_cag_custom",
      trim(col("acaacd")),
      trim(col("acaccd")),
      trim(col("acadcd")),
      lit("ERISA INDICATOR"),
      col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1472343_Udf() =
    length(
      trim(
        when(!isnull(tempExpression1211361_Udf().getField("tot_str_length")), tempExpression1421024_Udf())
          .otherwise(tempExpression1449284_Udf())
          .cast(StringType)
          .cast(StringType)
      )
    ) === lit(8)

  def tempExpression1181175_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            lit("*ALL"),
            lit("*ALL"),
            lit("ERISA INDICATOR"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1177950_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            lit("*ALL"),
            trim(col("acadcd")),
            lit("CARRIER ID"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1175458_Udf() =
    callUDF(
      "lkp_rcagfp_orx_d_cag_custom",
      trim(col("acaacd")),
      trim(col("acaccd")),
      trim(col("acadcd")),
      lit("SOLD MARKET SITE"),
      col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1371418_Udf() =
    when(
      !isnull(
        when(isnull(tempExpression1266972_Udf().getField("hm_dlvry_stat_ind")), lit("N"))
          .otherwise(tempExpression1266972_Udf().getField("hm_dlvry_stat_ind"))
      ),
      when(!lit(0).cast(BooleanType), lit(1).cast(BooleanType)).otherwise(tempExpression1327259_Udf())
    ).otherwise(tempExpression1327259_Udf())

  def tempExpression1427363_Udf() =
    when(
      isnull(
        string_substring(tempExpression1375733_Udf(),
                         tempExpression1238976_Udf().getField("starting_position"),
                         tempExpression1238976_Udf().getField("tot_str_length")
        )
      ),
      lit("-")
    ).otherwise(
      string_substring(tempExpression1375733_Udf(),
                       tempExpression1238976_Udf().getField("starting_position"),
                       tempExpression1238976_Udf().getField("tot_str_length")
      )
    )

  def tempExpression1163880_Udf() =
    callUDF("client_id_cag_mask",
            trim(col("acaacd")),
            when(isnull(trim(col("acaccd"))), lit("*")).otherwise(trim(col("acaccd"))),
            lit("*")
    )

  def tempExpression1251147_Udf() =
    callUDF(
      "lkp_rcagfp_orx_d_cag_custom",
      trim(col("acaacd")),
      trim(col("acaccd")),
      trim(col("acadcd")),
      lit("Specialty Eff Date"),
      col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1266196_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            trim(col("acaccd")),
            trim(col("acadcd")),
            lit("PRODUCT ID"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1172733_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            lit("*ALL"),
            trim(col("acadcd")),
            lit("SEGMENT INDICATOR"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1224921_Udf() =
    callUDF(
      "lkp_rcagfp_orx_d_cag_custom",
      trim(col("acaacd")),
      trim(col("acaccd")),
      trim(col("acadcd")),
      lit("COB IND / EFF DATE"),
      col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1225420_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            lit("*ALL"),
            lit("*ALL"),
            lit("COB IND / EFF DATE"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1327763_Udf() =
    when(
      !isnull(
        when(isnull(tempExpression1266972_Udf().getField("hm_dlvry_only_ind")), lit("N"))
          .otherwise(tempExpression1266972_Udf().getField("hm_dlvry_only_ind"))
      ),
      when(isnull(tempExpression1266972_Udf().getField("hm_dlvry_only_ind")), lit("N"))
        .otherwise(tempExpression1266972_Udf().getField("hm_dlvry_only_ind"))
    )

  def tempExpression2279919_Udf() =
    when(
      (col("dxf_src_sys_id").cast(IntegerType) === lit(490)).and(
        !isnull(
          callUDF("lkp_rcagfp_orx_d_cag_custom",
                  trim(col("acaacd")),
                  lit("*ALL"),
                  trim(col("acadcd")),
                  lit("POLICY NUMBER"),
                  col("dxf_src_sys_id").cast(IntegerType)
          ).getField("tot_str_length")
        )
      ),
      when(isnull(string_substring(col("acadcd"), lit(1), lit(7))), lit("-"))
        .otherwise(string_substring(col("acadcd"), lit(1), lit(7)))
    ).otherwise(
      when(
        (col("dxf_src_sys_id").cast(IntegerType) === lit(490)).and(
          !isnull(
            callUDF("lkp_rcagfp_orx_d_cag_custom",
                    trim(col("acaacd")),
                    trim(col("acaccd")),
                    lit("*ALL"),
                    lit("POLICY NUMBER"),
                    col("dxf_src_sys_id").cast(IntegerType)
            ).getField("tot_str_length")
          )
        ),
        when(isnull(string_substring(col("acadcd"), lit(1), lit(7))), lit("-"))
          .otherwise(string_substring(col("acadcd"), lit(1), lit(7)))
      ).otherwise(
        when(
          (col("dxf_src_sys_id").cast(IntegerType) === lit(490)).and(
            !isnull(
              callUDF("lkp_rcagfp_orx_d_cag_custom",
                      trim(col("acaacd")),
                      lit("*ALL"),
                      lit("*ALL"),
                      lit("POLICY NUMBER"),
                      col("dxf_src_sys_id").cast(IntegerType)
              ).getField("tot_str_length")
            )
          ),
          when(isnull(string_substring(col("acadcd"), lit(1), lit(7))), lit("-"))
            .otherwise(string_substring(col("acadcd"), lit(1), lit(7)))
        ).otherwise(lit("-"))
      )
    )

  def tempExpression1182416_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            lit("*ALL"),
            trim(col("acadcd")),
            lit("RENWAL DATE"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1327883_Udf() =
    when(
      !isnull(
        when(isnull(tempExpression1266972_Udf().getField("rebates_stat_ind")), lit("N"))
          .otherwise(tempExpression1266972_Udf().getField("rebates_stat_ind"))
      ),
      when(isnull(tempExpression1266972_Udf().getField("rebates_stat_ind")), lit("N"))
        .otherwise(tempExpression1266972_Udf().getField("rebates_stat_ind"))
    )

  def tempExpression1427313_Udf() =
    when(
      isnull(
        string_substring(tempExpression1375733_Udf(),
                         tempExpression1238731_Udf().getField("starting_position"),
                         tempExpression1238731_Udf().getField("tot_str_length")
        )
      ),
      lit("-")
    ).otherwise(
      string_substring(tempExpression1375733_Udf(),
                       tempExpression1238731_Udf().getField("starting_position"),
                       tempExpression1238731_Udf().getField("tot_str_length")
      )
    )

  def tempExpression1496235_Udf() =
    when(isnull(
           tempExpression1477380_Udf()
             .and(is_valid(when(tempExpression1477467_Udf(), tempExpression1477556_Udf())) === lit(1))
         ),
         lit(0).cast(BooleanType)
    ).otherwise(
      tempExpression1477380_Udf()
        .and(is_valid(when(tempExpression1477467_Udf(), tempExpression1477556_Udf())) === lit(1))
    )

  def tempExpression1206394_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            lit("*ALL"),
            trim(col("acadcd")),
            lit("LEGAL ENTITY"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1182661_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            lit("*ALL"),
            lit("*ALL"),
            lit("RENWAL DATE"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1264960_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            lit("*ALL"),
            trim(col("acadcd")),
            lit("GL PRODUCT CODE"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1164686_Udf() =
    string_substring(
      callUDF("lkp_rcagdp", col("acaacd"), lit("*ALL"), lit("*ALL"), col("dxf_src_sys_id").cast(IntegerType))
        .getField("h7ant4"),
      lit(2),
      length(
        callUDF("lkp_rcagdp", col("acaacd"), lit("*ALL"), lit("*ALL"), col("dxf_src_sys_id").cast(IntegerType))
          .getField("h7ant4")
      )
    )

  def tempExpression1525892_Udf() =
    when(isnull(
           (length(tempExpression1490296_Udf().cast(StringType)) === lit(8)).and(tempExpression1508508_Udf() === lit(1))
         ),
         lit(0).cast(BooleanType)
    ).otherwise(
      (length(tempExpression1490296_Udf().cast(StringType)) === lit(8)).and(tempExpression1508508_Udf() === lit(1))
    )

  def tempExpression1209374_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            lit("*ALL"),
            trim(col("acadcd")),
            lit("MIGRATION INDICATOR"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1494293_Udf() =
    is_valid(
      when(
        length(trim(tempExpression1474207_Udf().cast(StringType))) === lit(8),
        date_format(
          to_date(trim(tempExpression1474207_Udf().cast(StringType)).cast(StringType).cast(StringType), "yyyyMMdd"),
          "yyyyMMdd"
        )
      )
    )

  def tempExpression1165729_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            lit("*ALL"),
            lit("*ALL"),
            lit("Claim Suffix"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1173481_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            lit("*ALL"),
            trim(col("acadcd")),
            lit("HMO ACCOUNT DIV"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1414159_Udf() =
    when(
      isnull(
        string_substring(tempExpression1375733_Udf(),
                         tempExpression1181920_Udf().getField("starting_position"),
                         tempExpression1181920_Udf().getField("tot_str_length")
        )
      ),
      lit("-")
    ).otherwise(
      string_substring(tempExpression1375733_Udf(),
                       tempExpression1181920_Udf().getField("starting_position"),
                       tempExpression1181920_Udf().getField("tot_str_length")
      )
    )

  def tempExpression1239218_Udf() =
    callUDF(
      "lkp_rcagfp_orx_d_cag_custom",
      trim(col("acaacd")),
      trim(col("acaccd")),
      trim(col("acadcd")),
      lit("TRANS DATE TIMESTAMP"),
      col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1375733_Udf() =
    when(
      !isnull(
        callUDF("lkp_rcagdp", col("acaacd"), col("acaccd"), col("acadcd"), col("dxf_src_sys_id").cast(IntegerType))
          .getField("h7ant4")
      ),
      tempExpression1164554_Udf()
    ).otherwise(tempExpression1335055_Udf())

  def tempExpression1225175_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            lit("*ALL"),
            trim(col("acadcd")),
            lit("COB IND / EFF DATE"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1474207_Udf() =
    lit(19000000) + decimal_strip(
      when(!isnull(tempExpression1225666_Udf().getField("tot_str_length")), tempExpression1424499_Udf())
        .otherwise(tempExpression1451607_Udf())
        .cast(StringType)
    )

  def tempExpression1223930_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            lit("*ALL"),
            lit("*ALL"),
            lit("PRODUCT CLASS ID"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1175957_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            lit("*ALL"),
            lit("*ALL"),
            lit("SOLD MARKET SITE"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1164743_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            lit("*ALL"),
            trim(col("acadcd")),
            lit("CLAIM SUFFIX"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1176705_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            lit("*ALL"),
            lit("*ALL"),
            lit("SITUS STATE"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1414109_Udf() =
    when(
      isnull(
        string_substring(tempExpression1375733_Udf(),
                         tempExpression1181675_Udf().getField("starting_position"),
                         tempExpression1181675_Udf().getField("tot_str_length")
        )
      ),
      lit("-")
    ).otherwise(
      string_substring(tempExpression1375733_Udf(),
                       tempExpression1181675_Udf().getField("starting_position"),
                       tempExpression1181675_Udf().getField("tot_str_length")
      )
    )

  def tempExpression1264706_Udf() =
    callUDF(
      "lkp_rcagfp_orx_d_cag_custom",
      trim(col("acaacd")),
      trim(col("acaccd")),
      trim(col("acadcd")),
      lit("GL PRODUCT CODE"),
      col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1207884_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            lit("*ALL"),
            trim(col("acadcd")),
            lit("LEAD PARTNER"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1177696_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            trim(col("acaccd")),
            trim(col("acadcd")),
            lit("CARRIER ID"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1264457_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            lit("*ALL"),
            lit("*ALL"),
            lit("LOB"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1178195_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            lit("*ALL"),
            lit("*ALL"),
            lit("CARRIER ID"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1165978_Udf() =
    callUDF(
      "lkp_rcagfp_orx_d_cag_custom",
      trim(col("acaacd")),
      trim(col("acaccd")),
      trim(col("acadcd")),
      lit("Customer Number"),
      col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1171247_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            lit("*ALL"),
            trim(col("acadcd")),
            lit("PRODUCT CODE/TYPE"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1251646_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            lit("*ALL"),
            lit("*ALL"),
            lit("Specialty Eff Date"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1166477_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            lit("*ALL"),
            lit("*ALL"),
            lit("Customer Number"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1170993_Udf() =
    callUDF(
      "lkp_rcagfp_orx_d_cag_custom",
      trim(col("acaacd")),
      trim(col("acaccd")),
      trim(col("acadcd")),
      lit("PRODUCT CODE/TYPE"),
      col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1239472_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            lit("*ALL"),
            trim(col("acadcd")),
            lit("TRANS DATE TIMESTAMP"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1169551_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            lit("*ALL"),
            lit("*ALL"),
            lit("PLAN VARIATION"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1171492_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            lit("*ALL"),
            lit("*ALL"),
            lit("PRODUCT CODE/TYPE"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1265205_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            lit("*ALL"),
            lit("*ALL"),
            lit("GL PRODUCT CODE"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1493199_Udf() =
    when(isnull(
           tempExpression1472256_Udf()
             .and(is_valid(when(tempExpression1472343_Udf(), tempExpression1472432_Udf())) === lit(1))
         ),
         lit(0).cast(BooleanType)
    ).otherwise(
      tempExpression1472256_Udf()
        .and(is_valid(when(tempExpression1472343_Udf(), tempExpression1472432_Udf())) === lit(1))
    )

  def tempExpression1263958_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            trim(col("acaccd")),
            trim(col("acadcd")),
            lit("LOB"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1169306_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            lit("*ALL"),
            trim(col("acadcd")),
            lit("PLAN VARIATION"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression2281951_Udf() =
    when(
      !isnull(tempExpression1175216_Udf().getField("tot_str_length")),
      when(
        isnull(
          string_substring(tempExpression1375733_Udf(),
                           tempExpression1175216_Udf().getField("starting_position"),
                           tempExpression1175216_Udf().getField("tot_str_length")
          )
        ),
        lit("-")
      ).otherwise(
        string_substring(tempExpression1375733_Udf(),
                         tempExpression1175216_Udf().getField("starting_position"),
                         tempExpression1175216_Udf().getField("tot_str_length")
        )
      )
    ).otherwise(
      when(
        !isnull(tempExpression1175458_Udf().getField("tot_str_length")),
        when(
          isnull(
            string_substring(tempExpression1375733_Udf(),
                             tempExpression1175458_Udf().getField("starting_position"),
                             tempExpression1175458_Udf().getField("tot_str_length")
            )
          ),
          lit("-")
        ).otherwise(
          string_substring(tempExpression1375733_Udf(),
                           tempExpression1175458_Udf().getField("starting_position"),
                           tempExpression1175458_Udf().getField("tot_str_length")
          )
        )
      ).otherwise(
        when(
          !isnull(tempExpression1175712_Udf().getField("tot_str_length")),
          when(
            isnull(
              string_substring(tempExpression1375733_Udf(),
                               tempExpression1175712_Udf().getField("starting_position"),
                               tempExpression1175712_Udf().getField("tot_str_length")
              )
            ),
            lit("-")
          ).otherwise(
            string_substring(tempExpression1375733_Udf(),
                             tempExpression1175712_Udf().getField("starting_position"),
                             tempExpression1175712_Udf().getField("tot_str_length")
            )
          )
        ).otherwise(
          when(
            !isnull(tempExpression1175957_Udf().getField("tot_str_length")),
            when(
              isnull(
                string_substring(tempExpression1375733_Udf(),
                                 tempExpression1175957_Udf().getField("starting_position"),
                                 tempExpression1175957_Udf().getField("tot_str_length")
                )
              ),
              lit("-")
            ).otherwise(
              string_substring(tempExpression1375733_Udf(),
                               tempExpression1175957_Udf().getField("starting_position"),
                               tempExpression1175957_Udf().getField("tot_str_length")
              )
            )
          ).otherwise(lit(null))
        )
      )
    )

  def tempExpression1477556_Udf() =
    date_format(
      to_date(
        trim(
          when(!isnull(tempExpression1251147_Udf().getField("tot_str_length")), tempExpression1430419_Udf())
            .otherwise(tempExpression1455045_Udf())
            .cast(StringType)
            .cast(StringType)
        ).cast(StringType).cast(StringType),
        "yyyyMMdd"
      ),
      "yyyyMMdd"
    )

  def tempExpression1164554_Udf() =
    string_substring(
      callUDF("lkp_rcagdp", col("acaacd"), col("acaccd"), col("acadcd"), col("dxf_src_sys_id").cast(IntegerType))
        .getField("h7ant4"),
      lit(2),
      length(
        callUDF("lkp_rcagdp", col("acaacd"), col("acaccd"), col("acadcd"), col("dxf_src_sys_id").cast(IntegerType))
          .getField("h7ant4")
      )
    )

  def tempExpression2287947_Udf() =
    date_format(
      to_timestamp(
        when(
          (((!isnull(
            when(!isnull(tempExpression1237736_Udf().getField("tot_str_length")), tempExpression1427113_Udf())
              .otherwise(tempExpression1512230_Udf())
          )).and(
              !is_blank(
                when(!isnull(tempExpression1237736_Udf().getField("tot_str_length")), tempExpression1427113_Udf())
                  .otherwise(tempExpression1512230_Udf())
              )
            ))
            .and(
              when(!isnull(tempExpression1237736_Udf().getField("tot_str_length")), tempExpression1427113_Udf())
                .otherwise(tempExpression1512230_Udf()) =!= lit("-")
            ))
            .and(
              string_is_numeric(
                when(!isnull(tempExpression1237736_Udf().getField("tot_str_length")), tempExpression1427113_Udf())
                  .otherwise(tempExpression1512230_Udf())
              ).cast(BooleanType)
            ),
          date_format(
            to_timestamp(
              truncateMicroSeconds(
                lit("yyyyMMddHHmmssSSSSSS"),
                string_pad(trim(
                             when(!isnull(tempExpression1237736_Udf().getField("tot_str_length")),
                                  tempExpression1427113_Udf()
                             ).otherwise(tempExpression1512230_Udf())
                           ),
                           20,
                           "0"
                )
              ).cast(StringType),
              "yyyyMMddHHmmss"
            ),
            "yyyyMMddHHmmss"
          )
        ).otherwise(lit("19000101000000")).cast(StringType),
        "yyyyMMddHHmmss"
      ),
      "yyyyMMddHHmmss"
    )

  def tempExpression1434629_Udf() =
    when(tempExpression1403201_Udf(),
         when(isnull(array(lit("N"), lit("N"), lit("N"), lit("N"))), array())
           .otherwise(array(lit("N"), lit("N"), lit("N"), lit("N")))
    ).otherwise(tempExpression1371628_Udf())

  def tempExpression1263471_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            lit("*ALL"),
            trim(col("acadcd")),
            lit("Pick LOB"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1174971_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            lit("*ALL"),
            trim(col("acadcd")),
            lit("SOLD MARKE SITE"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1282177_Udf() =
    when(!isnull(callUDF("client_id_cag_mask", trim(col("acaacd")), lit("*"), lit("*")).getField("client_id")),
         tempExpression1163939_Udf()
    ).otherwise(lit("OPTUM"))

  def tempExpression1181421_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            trim(col("acaccd")),
            trim(col("acadcd")),
            lit("RENEWAL DATE"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1178441_Udf() =
    callUDF("lkp_rcagfp_orx_d_cag_custom",
            trim(col("acaacd")),
            trim(col("acaccd")),
            trim(col("acadcd")),
            lit("SA INDICATOR"),
            col("dxf_src_sys_id").cast(IntegerType)
    )

  def tempExpression1424599_Udf() =
    when(
      isnull(
        string_substring(tempExpression1375733_Udf(),
                         tempExpression1226165_Udf().getField("starting_position"),
                         tempExpression1226165_Udf().getField("tot_str_length")
        )
      ),
      lit("-")
    ).otherwise(
      string_substring(tempExpression1375733_Udf(),
                       tempExpression1226165_Udf().getField("starting_position"),
                       tempExpression1226165_Udf().getField("tot_str_length")
      )
    )

  def tempExpression1173227_Udf() =
    callUDF(
      "lkp_rcagfp_orx_d_cag_custom",
      trim(col("acaacd")),
      trim(col("acaccd")),
      trim(col("acadcd")),
      lit("HMO ACCOUNT DIV"),
      col("dxf_src_sys_id").cast(IntegerType)
    )

}
