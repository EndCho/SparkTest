package xiaoniu.sparkSql

import org.apache.spark.sql.{DataFrame, SparkSession}

object ParquetDataSource {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    // 指定以后读取csv类型的数据(无表头)
    val parquet: DataFrame = spark.read.parquet("C:\\Users\\txdyl\\Desktop\\log\\out\\parquet")

    spark.close()

  }
}
