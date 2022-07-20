package graph

import io.prophecy.libs._
import io.prophecy.libs.SparkFunctions._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

@Visual(id = "Optional_Filter__Optional_Filter", label = "Optional_Filter.Optional_Filter", x = 100, y = 100, phase = 1)
object Optional_Filter__Optional_Filter {

  def apply(spark: SparkSession, in: DataFrame): (RowDistributor, RowDistributor) = {

    val out0 = in.filter(
      (!isnull(col("acaacd"))).and(
        !string_like(upper(col("acaacd")), lit("%TEST%"))
          .cast(BooleanType)
          .or(string_like(upper(col("acaacd")), lit("%TST%")).cast(BooleanType))
      )
    )
    val out1 = in.filter(
      !(!isnull(col("acaacd"))).and(
        !string_like(upper(col("acaacd")), lit("%TEST%"))
          .cast(BooleanType)
          .or(string_like(upper(col("acaacd")), lit("%TST%")).cast(BooleanType))
      )
    )

    (out1, out0)

  }

}
