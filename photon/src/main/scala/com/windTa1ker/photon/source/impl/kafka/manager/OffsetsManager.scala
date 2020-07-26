package com.windTa1ker.photon.source.impl.kafka.manager

import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf

/**
 * @time: 2020/7/2 12:32 下午
 * @author: 荆旗
 * @Description:
 */
trait OffsetsManager extends Serializable {

  val sparkConf: SparkConf

  lazy val storeParams: Map[String, String] = sparkConf
  .getAllWithPrefix(s"spark.source.kafka.offset.store.")
  .toMap

  lazy val storeType: String = storeParams.getOrElse("type", "none")

  /**
   * 获取存储的Offset
   *
   * @param groupId
   * @param topics
   * @return
   */
  def getOffsets(groupId: String, topics: Set[String]): Map[TopicPartition, Long]

  /**
   * 更新 Offsets
   *
   * @param groupId
   * @param offsetInfos
   */
  def updateOffsets(groupId: String, offsetInfos: Map[TopicPartition, Long]): Unit

  /**
   * 删除 Offsets
   *
   * @param groupId
   * @param topics
   */
  def delOffsets(groupId: String, topics: Set[String]): Unit = {}

  /**
   * 生成Key
   *
   * @param groupId
   * @param topic
   * @return
   */
  def generateKey(groupId: String, topic: String): String = s"$groupId#$topic"

}