package com.windTa1ker.photon

import com.windTa1ker.photon.utils.Utils
import org.apache.spark.SparkConf

import scala.collection.mutable.ArrayBuffer

/**
 * time: 2020/7/2 1:37 下午
 * author: 荆旗
 * Description:
 */
trait PhotonBase {

  protected val sparkListeners = new ArrayBuffer[String]()

  /**
   * 初始化，函数，可以设置 sparkConf
   *
   * @param propFile 配置文件名
   */
  def init(propFile: String): SparkConf = {
    val sparkConf = new SparkConf()
    propFile match {
      case null => sparkConf
      case _ =>
        val props = Utils.getPropertiesFromFile(propFile)
        sparkConf.setAll(props)
    }
  }
}
