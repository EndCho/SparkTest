package xiaoniu.sparkSql

import org.apache.spark.sql.{DataFrame, SparkSession}

object JoinTest {
  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder().
      appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._

    //spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 1024*1024*100)
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1) // 一个字节都不广播

    val df1: DataFrame = Seq(
      (0, "playing"),
      (1, "with"),
      (2, "join")
    ).toDF("id", "token")

    val df2: DataFrame = Seq(
      (0, "p"),
      (1, "w"),
      (2, "s")
    ).toDF("aid", "atoken")


    val result: DataFrame = df1.join(df2, $"id"===$"aid")
    // 查看执行计划
    result.explain()
    result.show()
    spark.close()
  }

}
