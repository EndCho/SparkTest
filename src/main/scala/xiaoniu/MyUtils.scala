package xiaoniu

import java.sql.DriverManager

import scala.io.{BufferedSource, Source}

object MyUtils {

  def ip2Long(ip: String): Long = {
    val fragments = ip.split("[.]")
    var ipNum = 0L
    for (i <-0 until fragments.length){
      ipNum = fragments(i).toLong | ipNum << 8L
    }
    ipNum
  }


  def binarySeach(lines:Array[(Long, Long, String)], ip: Long): Int={
    var low = 0
    var hight = lines.length -1
    while (low < hight){
      val middle = (low + hight)/2
      if ((ip >= lines(middle)._1) && (ip <= lines(middle)._2))
        return middle
      if (ip < lines(middle)._1)
        hight = middle-1
      else {
        low =middle + 1
      }
    }
    -1
  }

  def readRules(path: String): Array[(Long, Long, String)]={

    // 读取ip规则
    val bf: BufferedSource = Source.fromFile(path)
    val lines: Iterator[String] = bf.getLines()
    // 对ip规则进行整理，并放入到内存
    val rules: Array[(Long, Long, String)] = lines.map(line => {
      val fieds = line.split("[|]")
      val startNum = fieds(2).toLong
      val endNum = fieds(3).toLong
      val province = fieds(6)
      (startNum, endNum, province)
    }).toArray
    rules

  }

  def data2MySQL(it:Iterator[(String,Int)])={
    val conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata?useUnicode=true&characterEncoding=utf-8","root","root")
    // 将数据通过Conncetion写入到数据库
    val pstm = conn.prepareStatement("insert into access_log values(?,?)")
    // 将一个分区中的第一条数据拿出来
    it.foreach(tp=>{
      pstm.setString(1, tp._1)
      pstm.setInt(2, tp._2)
      pstm.executeLargeUpdate()
    })
    if (pstm != null)
      pstm.close()
    if (conn != null)
      conn.close()
  }

  def main(args: Array[String]): Unit = {

    val rules = readRules("C:\\Users\\txdyl\\Desktop\\log\\in\\ip.txt")

    val ipNum = ip2Long("202.96.128.86")
    val index = binarySeach(rules,ipNum)
    println(index)

    println(rules(index))
  }
}
