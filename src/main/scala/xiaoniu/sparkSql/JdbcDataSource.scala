package xiaoniu.sparkSql

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object JdbcDataSource {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()
    val orders: DataFrame = spark.read.format("jdbc").options(
      Map("url" -> "jdbc:mysql://localhost:3306/ssm",
        "dirver" -> "com.mysql.jdbc.Driver",
        "dbtable"->"t_order",
        "user" -> "root",
        "password" -> "root"
      )).load()
    // orders.printSchema()

    //orders.show()

//    val filtered: Dataset[Row] = orders.filter(o => {
//      o.getAs[Long]("uid") >= 8
//    })

    // lambda表达式
    // val filtered = orders.filter($"uid" >= 8)
    val filtered = orders.where($"uid" >=8)

    orders.select($"uid")
    filtered.show()

    spark.close()
  }

}
