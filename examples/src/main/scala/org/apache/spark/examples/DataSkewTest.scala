package org.apache.spark.examples

import org.apache.spark.{SparkConf, SparkContext}

import java.util

/**
 * @DESC: 一个数据倾斜的例子
 * 统计出每个上网的目的IP(target_ip)，都是通过哪些客户端IP(client_ip)登录的，
 * 以及每个客户端IP的登录次数分别是多少?
 */
//noinspection ScalaStyle
object DataSkewTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DataSkewTest01").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rawRDD = sc.textFile("D:\\test.txt")//读取数据源

    val reducedRDD = rawRDD.map(line => {/**根据目的ip进行汇总，将访问同一个目的ip的所有客户端ip进行汇总*/
      val array = line.split(",")
      val target_ip = array(0)
      val client_ip = array(1)
      val index = client_ip.lastIndexOf(".")
      val subClientIP = client_ip.substring(0, index) //为了让后续聚合后的value数据量尽可能的少，只取ip的前段部分
      (target_ip,Array(subClientIP))
    }).reduceByKey(_++_)//将Array中的元素进行合并，然后将分区调整为已知的4个

    val targetRDD_01 = reducedRDD.map(kv => {/**第二步：将各个分区中的数据进行初步统计，减少单个分区中value的大小*/
      val map = new util.HashMap[String,Int]()
      val target_ip = kv._1
      val clientIPArray = kv._2
      clientIPArray.foreach(clientIP => {//对clientIP进行统计
        if (map.containsKey(clientIP)) {
          val sum = map.get(clientIP) + 1
          map.put(clientIP,sum)
        }
        else map.put(clientIP,1)
      })
      (target_ip,map)
    })
    // 打印结果时，将数组转为字符串
    targetRDD_01.saveAsTextFile("r7")

    System.in.read
    sc.stop()
  }
}

