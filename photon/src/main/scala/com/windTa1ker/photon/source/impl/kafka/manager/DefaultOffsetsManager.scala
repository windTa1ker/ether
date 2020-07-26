package com.windTa1ker.photon.source.impl.kafka.manager

import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf

/**
 * @time: 2020/7/2 12:32 下午
 * @author: 荆旗
 * @Description:
 */
class DefaultOffsetsManager(val sparkConf: SparkConf) extends OffsetsManager {

  /**
   * 获取存储的Offset
   *
   * @param groupId
   * @param topics
   * @return
   */
  override def getOffsets(groupId: String, topics: Set[String]): Map[TopicPartition, Long] = {
    Map.empty[TopicPartition, Long]
  }

  /**
   * 更新 Offsets
   *
   * @param groupId
   * @param offsetInfos
   */
  override def updateOffsets(groupId: String, offsetInfos: Map[TopicPartition, Long]): Unit = {

  }
}