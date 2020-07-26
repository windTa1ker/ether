package com.windTa1ker.photon

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/**
 * time: 2020/7/2 6:48 下午
 * author: 荆旗
 * Description: 
 */
trait PhotonSQL extends PhotonBase {
  /**
   * spark 启动后 调用
   */
  def afterStarted(sc: SparkSession): Unit = {}

  /**
   * spark 停止后 程序结束前 调用
   */
  def beforeStop(sc: SparkSession): Unit = {}

  /**
   * 处理函数
   *
   * @param spark spark entry
   */
  def handle(spark: SparkSession): Unit

  def creatingSession(propFile: String): SparkSession = {
    val sparkConf = init(propFile)
    val extraListeners = sparkListeners.mkString(",") + "," + sparkConf.get("spark.extraListeners", "")
    val isHiveEnabled = sparkConf.get("spark.sql.hive.enable","false").toBoolean
    if (extraListeners != ",") sparkConf.set("spark.extraListeners", extraListeners)
    val spark = if(isHiveEnabled) {
      SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    }else{
      SparkSession.builder().config(sparkConf).getOrCreate()
    }
    handle(spark)
    spark
  }

  def main(args: Array[String]): Unit = {

    val propFile = if(args.length > 0) args(0) else null

    val spark = creatingSession(propFile)
    afterStarted(spark)
    spark.stop()
    beforeStop(spark)
  }


}
