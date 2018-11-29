package xiaoniu.sparkSql

import org.apache.spark.sql.{DataFrame, SparkSession}

object CsvDataSource {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    // 指定以后读取csv类型的数据(无表头)
    val csv: DataFrame = spark.read.csv("C:\\Users\\txdyl\\Desktop\\log\\out\\csv")
    val pDF = csv.toDF("oid","gen_time","total_price","uid")
    pDF.show()
    spark.close()

  }

}
