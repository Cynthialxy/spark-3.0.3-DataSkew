
// scalastyle:off println
package org.apache.spark.examples

import org.apache.spark.{SparkConf, SparkContext}

//noinspection ScalaStyle
object Sort {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
       .setMaster("local[*]")
       .setAppName("sort")

    // 构造一个用于运行spark程序的上下文对象
    val sc = new SparkContext(conf)
    println("defaultParallelism: "+sc.defaultParallelism)

//    val input=args(0)
//val input="D:\\testData\\bigdata.txt"
val input="D:\\wc.txt"
//val input="hdfs://localhost:9000/bigdata.txt"

//    sc.parallelize()
    val rdd = sc.textFile(input,20)
      .flatMap(_.split("\\s+"))
      .map((_, 1))
      .sortByKey()
      .reduceByKey(_+_)

//      rdd.count()
    rdd.foreach(println(_))

    System.in.read
    sc.stop()
  }
}
