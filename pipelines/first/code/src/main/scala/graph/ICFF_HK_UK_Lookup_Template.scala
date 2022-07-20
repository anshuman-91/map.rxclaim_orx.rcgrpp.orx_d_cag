package graph

import io.prophecy.libs._
import io.prophecy.libs._
import org.apache.spark.sql._
import org.apache.spark.sql.types._

@Visual(id = "ICFF_HK_UK_Lookup_Template", label = "ICFF_HK_UK_Lookup_Template", x = 100, y = 100, phase = -2147483648)
object ICFF_HK_UK_Lookup_Template {

  @UsesDataset(id = "1", version = 0)
  def apply(spark: SparkSession): Lookup = {

    val fabric = "default"

    lazy val out = fabric match {
      case "default" =>
        val schemaArg = StructType(
          Array(StructField("dxf_hk_part1", IntegerType, true),
                StructField("dxf_hk_part2", IntegerType, true),
                StructField("dxf_sk",       IntegerType, true)
          )
        )

        spark.read.schema(schemaArg).parquet("dbfs:/FileStore/optum/ICFF_HK_UK_Lookup_Template.parquet")
          .cache()
      case _ => throw new Exception(s"The fabric is not handled")
    }
    createLookup("ICFF_HK_UK_Lookup_Template",
                 out,
                 spark,
                 List("dxf_hk_part1"),
                 "dxf_hk_part1",
                 "dxf_hk_part2",
                 "dxf_sk"
    )

  }

}
