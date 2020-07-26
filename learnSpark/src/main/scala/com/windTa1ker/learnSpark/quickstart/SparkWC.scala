package com.windTa1ker.learnSpark.quickstart

import com.windTa1ker.photon.PhotonCore
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @time: 2020/7/1 1:20 下午
 * @author: 荆旗
 * @Description: 从 spark 源码中拷贝过来，用来测试本地环境, 并做简单改动
 */
object SparkWC extends PhotonCore{

  /**
   * 处理函数
   *
   * @param sc spark context
   */

  override def handle(sc: SparkContext): Unit = {
    val inputFile = "file:///Users/windta1ker/temp/xxx.sh"
    val input = sc.textFile(inputFile)
    val words = input.flatMap(line => line.split(" "))
    val counts = words.map(word => (word, 1)).reduceByKey({ case (x, y) => x + y })
    counts.foreach(println)
  }
}