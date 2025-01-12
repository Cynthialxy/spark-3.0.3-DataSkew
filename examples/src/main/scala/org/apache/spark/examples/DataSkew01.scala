package org.apache.spark.examples

import org.apache.spark.{SparkConf, SparkContext}

import java.util

/**
 * @DESC: 一个数据倾斜的例子
 * 统计出每个上网的目的IP(target_ip)，都是通过哪些客户端IP(client_ip)登录的，
 * 以及每个客户端IP的登录次数分别是多少?
 */
//noinspection ScalaStyle
object DataSkew01 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DataSkewTest01").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rawRDD = sc.textFile("D:\\data_skew5gb.txt")//读取数据源

    val reducedRDD = rawRDD.map(line => {/**根据目的ip进行汇总，将访问同一个目的ip的所有客户端ip进行汇总*/
        val array = line.split(",")
        val target_ip = array(0)
        val client_ip = array(1)
        val index = client_ip.lastIndexOf(".")
        val subClientIP = client_ip.substring(0, index) //为了让后续聚合后的value数据量尽可能的少，只取ip的前段部分
        (target_ip,Array(subClientIP))
      }).groupByKey(4)
      .reduceByKey(_++_,4)//将Array中的元素进行合并，然后将分区调整为已知的4个

    val targetRDD = reducedRDD.map(kv => {/**将访问同一个目的ip的客户端，再次根据客户端ip进行进一步统计*/
      val map = new util.HashMap[String,Int]()
      val target_ip = kv._1
      val clientIPArray = kv._2.flatten
      clientIPArray.foreach(clientIP => {
        if (map.containsKey(clientIP)) {
          val sum = map.get(clientIP) + 1
          map.put(clientIP,sum)
        }
        else map.put(clientIP,1)
      })
      (target_ip,map)
    })

    targetRDD.saveAsTextFile("r2-5g") //结果数据保存目录

    System.in.read
    sc.stop()
  }
}

