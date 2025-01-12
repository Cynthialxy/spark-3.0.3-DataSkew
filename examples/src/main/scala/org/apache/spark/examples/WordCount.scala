
// scalastyle:off println
package org.apache.spark.examples

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

//noinspection ScalaStyle
object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
       .setMaster("local")
       .setAppName("wordCount")

    // 构造一个用于运行spark程序的上下文对象
    val sc = new SparkContext(conf)

    val mapres=sc.textFile("D:/wc.txt")
      .flatMap(_.split("\\s+"))
      .map((_,1))

    val reduceres=mapres
      .reduceByKey(_+_)

    reduceres.foreach(println(_))

    System.in.read
    sc.stop()
  }
}
