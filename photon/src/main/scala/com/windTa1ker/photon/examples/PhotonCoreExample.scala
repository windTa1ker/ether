package com.windTa1ker.photon.examples

import com.windTa1ker.photon.PhotonCore
import org.apache.spark.SparkContext

/**
 * @time: 2020/7/1 2:29 下午
 * @author: 荆旗
 * @Description:
 */
object PhotonCoreExample extends PhotonCore{
  /**
   * 处理函数
   *
   * @param sc
   */
  override def handle(sc: SparkContext): Unit = {
    val inputFile = "file:///Users/windta1ker/temp/xxx.sh"
    val input = sc.textFile(inputFile)
    val words = input.flatMap(line => line.split(" "))
    val counts = words.map(word => (word, 1)).reduceByKey({ case (x, y) => x + y })
    counts.foreach(println)
  }
}
