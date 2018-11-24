package xiaoniu

import java.net.URL


object TestSplit {
  def main(args: Array[String]): Unit = {
    val line = "http://bigdata.rz.com/zc"
    // 学科 老师
//    val splits: Array[String] = line.split("/")
//    val subject = splits(2).split("[.]")(0)
//    val teacher = splits(3)
//    println(subject + " " + teacher)


    val index: Int = line.lastIndexOf("/")
    val teacher = line.substring(index+1)
    val httpHost = line.substring(0,index)
    val subject = new URL(httpHost).getHost.split("[.]")(0)
    print(teacher + " " + subject)

  }
}
