package graph

import io.prophecy.libs._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

@Visual(id = "ILM_Logical_Split_Files__FBE_Drop_Records_with_drop_record_1",
        label = "ILM_Logical_Split_Files.FBE_Drop_Records_with_drop_record_1",
        x = 100,
        y = 100,
        phase = 1
)
object ILM_Logical_Split_Files__FBE_Drop_Records_with_drop_record_1 {

  def apply(spark: SparkSession, in: DataFrame): Filter = {

    val out = in.filter(col("ctrx_d_cag.drop_record") =!= lit(2))

    out

  }

}
