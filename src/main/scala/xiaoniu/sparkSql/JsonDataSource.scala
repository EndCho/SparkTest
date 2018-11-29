package xiaoniu.sparkSql

import org.apache.spark.sql.{DataFrame, SparkSession}

object JsonDataSource {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    // 指定以后读取json类型的数据(有表头)
    val jsobs: DataFrame = spark.read.json("C:\\Users\\txdyl\\Desktop\\log\\out\\t_order.json")
    jsobs.show()
    spark.close()
    
  }

}
