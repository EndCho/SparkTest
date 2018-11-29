package xiaoniu.sparkSql

import java.util.Properties

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object JdbcDataSource {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._
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

//    val filtered: Dataset[Row] = orders.filter(o => {
//      o.getAs[BigDecimal]("total_price") >= 0
//    })

      //orders.filter(_.getAs[java.math.BigDecimal]("") >= 0)
    // lambda表达式
     val filtered = orders.filter($"uid" >= 8)

    //    val filtered = orders.where($"uid" >=8)
    val r: DataFrame = filtered.select($"oid",$"gen_time",$"total_price"*3 as "total_price", $"uid")
    r.show()

    val properties = new Properties()
    properties.put("user","root")
    properties.put("password","root")
    // 使用append模式不能添加自增id
    //r.write.mode("append").jdbc("jdbc:mysql://localhost:3306/ssm", "t_order", properties)

    // 将数据写入到mysql表
    //r.write.mode("ignore").jdbc("jdbc:mysql://localhost:3306/ssm", "t_order_new", properties)
    //orders.select($"uid")
//    filtered.show()
    //r.write.json("C:\\Users\\txdyl\\Desktop\\log\\out\\t_order.json")
    //r.write.csv("C:\\Users\\txdyl\\Desktop\\log\\out\\csv")
    //r.write.parquet("C:\\Users\\txdyl\\Desktop\\log\\out\\parquet")
    r.write.parquet("hdfs://hdp-01:9000/Users/txdyl/Desktop/log/out/parquet")




    spark.close()
  }

}
