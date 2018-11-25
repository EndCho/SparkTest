package xiaoniu.sparkSql

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object JoinTes {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val lines: Dataset[String] = spark.createDataset(List("1,laowang,china", "2,niconiconi,japen", "3,Trump,usa","4,meigen,uk"))

    // 对数据进行整理
    val tpDs: Dataset[(Long, String, String)] = lines.map(line => {
      val fields = line.split(",")
      val id = fields(0).toLong
      val name = fields(1)
      val naction = fields(2)
      (id, name, naction)
    })
    val df1: DataFrame = tpDs.toDF("id","name","naction")

    val nactions: Dataset[String] = spark.createDataset(List("china,中国", "japen,日本", "usa,美国"))
    // 对数据进行整理
    val ndataset: Dataset[(String, String)] = nactions.map(line => {
      val fields = line.split(",")
      val ename = fields(0)
      val cname = fields(1)
      (ename, cname)
    })
    val df2: DataFrame = ndataset.toDF("ename", "cname")

    // 第一种，创建视图
//    df1.createTempView("v_users")
//    df2.createTempView("v_nactions")
//    val result: DataFrame = spark.sql("select name, cname from v_users join v_nactions on naction = ename")


    val result = df1.join(df2,$"naction" === $"ename","left")

    result.show()
    spark.close()
  }
}
