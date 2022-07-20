package graph

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object SparkAbinitioDataFrameComparator {

  def compareFlattenedDataFrame(spark: SparkSession, sparkDF: DataFrame, abinitioDF: DataFrame): Unit = {

    val columnsAbi = abinitioDF.columns.map(_.toLowerCase).toSet

    val _dfAbi = abinitioDF.select(columnsAbi.map(c => col(c).as(s"abi_$c")).toSeq: _*)
    val dfJoined = sparkDF.join(_dfAbi, col("ctrx_d_cag.dxf_src_rec_cnt") === col("abi_ctrx_d_cag.dxf_src_rec_cnt") && col("ctrx_d_cag.dxf_src_sys_id") === col("abi_ctrx_d_cag.dxf_src_sys_id")).cache()

    val dfMatches = {
      dfJoined
        .select(
          (dfJoined.columns.map(c => col(c)).toList :::
            dfJoined.columns
              .filter(!_.startsWith("abi_"))
              .map { c =>
                when((col(c).isNull && col(s"abi_$c").isNull) || (col(c) === col(s"abi_$c")), lit("MATCH")).otherwise(lit("MISMATCH")).as(s"match_$c")
              }.toList): _*
        )
    }
    val dfMisMatches = dfMatches.filter(
      dfMatches.columns.filter(_.startsWith("match_")).map(col(_) === "MISMATCH").reduce(_ or _)
    )
    dfMisMatches.count()
  }

  def compare_Ctrx_D_Cag_Column(spark: SparkSession,_sparkDF: DataFrame, _abinitioDF: DataFrame): Unit = {
    import spark.implicits._
    val abinitioDF = _abinitioDF.select($"ctrx_d_cag.*")
    val columnsAbi = abinitioDF.columns.map(_.toLowerCase).toSet
    val sparkDF = _sparkDF.select($"ctrx_d_cag.*")
    val _dfAbi = abinitioDF.select(columnsAbi.map(c => col(c).as(s"abi_$c")).toSeq: _*)
    val dfJoined = sparkDF.join(_dfAbi, col("dxf_src_rec_cnt") === col("abi_dxf_src_rec_cnt") && col("dxf_src_sys_id") === col("abi_dxf_src_sys_id")).cache()

    val dfMatches = {
      dfJoined
        .select(
          (dfJoined.columns.map(c => col(c)).toList :::
            dfJoined.columns
              .filter(!_.startsWith("abi_"))
              .map { c =>
                when((col(c).isNull && col(s"abi_$c").isNull) || (col(c) === col(s"abi_$c")), lit("MATCH")).otherwise(lit("MISMATCH")).as(s"match_$c")
              }.toList): _*
        )
    }


    val dfMisMatchesOnly = dfMatches.select(
      dfMatches.columns.filter(_.startsWith("match_")).map(c => col(c)).toSeq: _*
    ).cache()

    dfMisMatchesOnly.columns.foreach { column =>
      val tmpDF = dfMisMatchesOnly.groupBy(column).agg(
        count(when(col(column) === lit("MATCH"), true)).as("match_count"),
        count(when(col(column) === lit("MISMATCH"), true)).as("mismatch_count")
      )
      val row = tmpDF.collect().head
      if(row.getAs[Int]("mismatch_count") > 0) {
        tmpDF.show(false)
      }
    }


  }

}
