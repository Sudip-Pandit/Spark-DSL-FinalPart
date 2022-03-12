package Final_dsl_Pkg

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr, lit}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions.{concat, lit}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

object dslSpecific {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DSL-APP").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    sc.setLogLevel("ERROR")
    println("=====read the datasets=======")
    val df = spark.read.format("csv").option("header","true").option("delimiter","~").load("file:///C:/data/txns_head")
    df.show()

    println("======using select expression========")
    val df1 = df
      .selectExpr(
        "txnno",
        "split(txndate, '-')[2] as year",
        "custno",
        "amount",
        "category",
        "product",
        "city",
        "state",
        "case when spendby='credit' then 1 else 0 end as check"

      )
    println("=====select expression======")
    df1.show()
    val df2 = df.withColumn("txndate", expr("split(txndate, '-')[2]"))
    df2.show()

    println("======column name changed/column renamed======")
      val df3 = df2
      .withColumnRenamed("txndate","year")
    df3.show()

    println("======concat two columns=======")

    val concatdf = df.withColumn("city", expr("concat(city, '-', state)"))
      .withColumnRenamed("city", "location").drop(col("state"))

    concatdf.show()

    println("======using groupby functions/how to alias the aggregated column, change datatype and alias======")
    val aggdf = df.groupBy("spendby").agg(sum("amount").cast(IntegerType).alias("total"))
    aggdf.show()


  }

}
