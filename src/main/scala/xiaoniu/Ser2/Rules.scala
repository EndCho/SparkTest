package xiaoniu.Ser2

import java.net.InetAddress


// 第三中方式，希望Rules在Executor中被初始化（不走网络了，就不必实现序列化接口了）
object Rules{
  val rulesMap = Map("hadoop"->2.7, "spark"->2.2)

  val hostName: String = InetAddress.getLocalHost.getHostName
  println(hostName + "@@@@@@@@@@@@@@ ! ! ! !")
}
