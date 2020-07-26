package com.windTa1ker.photon

import org.apache.spark.SparkContext

/**
 * time: 2020/7/1 2:22 下午
 * author: 荆旗
 * Description: Spark core 入口
 */

trait PhotonCore extends PhotonBase {

  /**
   * spark 启动后 调用
   */
  def afterStarted(sc: SparkContext): Unit = {}

  /**
   * spark 停止后 程序结束前 调用
   */
  def beforeStop(sc: SparkContext): Unit = {}

  /**
   * 处理函数
   *
   * @param sc spark context
   */
  def handle(sc: SparkContext): Unit

  def creatingContext(propFile: String): SparkContext = {
    val sparkConf = init(propFile)
    val extraListeners = sparkListeners.mkString(",") + "," + sparkConf.get("spark.extraListeners", "")
    if (extraListeners != ",") sparkConf.set("spark.extraListeners", extraListeners)
    val sc = new SparkContext(sparkConf)
    handle(sc)
    sc
  }

  def main(args: Array[String]): Unit = {

    val propFile = if(args.length > 0) args(0) else null

    val context = creatingContext(propFile)
    afterStarted(context)
    context.stop()
    beforeStop(context)
  }

}
