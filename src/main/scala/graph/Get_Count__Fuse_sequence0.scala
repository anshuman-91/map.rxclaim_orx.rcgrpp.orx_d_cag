package graph

import io.prophecy.libs.Component._
import org.apache.spark.sql.ProphecyDataFrame._
import org.apache.spark.sql._

@Visual(id = "Get_Count__Fuse_sequence0", label = "Get_Count.Fuse_sequence0", x = 100, y = 100, phase = 3)
object Get_Count__Fuse_sequence0 {

  def apply(spark: SparkSession, in: DataFrame): Sequence = {

    lazy val out = in.zipWithIndex(0, 1, "id", spark)

    out

  }

}
