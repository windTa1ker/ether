package com.windTa1ker.photon.source.impl.kafka.manager

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies, LocationStrategy, OffsetRange}
import java.{util => ju}

import org.apache.spark.rdd.RDD
/**
 * @time: 2020/7/2 12:29 下午
 * @author: 荆旗
 * @Description:
 */
class KafkaManager(val sparkConf: SparkConf) extends Serializable {


  // 自定义
  private lazy val offsetsManager = {
    sparkConf.get("spark.source.kafka.offset.store.type", "none").trim.toLowerCase match {
      case "redis" => new RedisOffsetsManager(sparkConf)
      case "redis-cluster" => new RedisClusterOffsetsManager(sparkConf)
      case "kafka" => new DefaultOffsetsManager(sparkConf)
      case "none" => new DefaultOffsetsManager(sparkConf)
    }
  }


  def offsetManagerType: String = offsetsManager.storeType


  /**
   * 从Kafka创建一个 InputDStream[ConsumerRecord[K, V]]
   *
   * @param ssc
   * @param kafkaParams
   * @param topics
   * @tparam K
   * @tparam V
   * @return
   */
  def createDirectStream[K, V](ssc: StreamingContext,
    kafkaParams: Map[String, Object],
    topics: Set[String]
  ): InputDStream[ConsumerRecord[K, V]] = {
    var consumerOffsets = Map.empty[TopicPartition, Long]
    kafkaParams.get("group.id") match {
      case Some(groupId) =>
        consumerOffsets = offsetsManager.getOffsets(groupId.toString, topics)
      case _ =>
    }
    KafkaUtils.createDirectStream[K, V](ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[K, V](topics, kafkaParams, consumerOffsets))
  }

  /**
   *
   * @param sc
   * @param kafkaParams
   * @param offsetRanges
   * @param locationStrategy
   * @tparam K
   * @tparam V
   * @return
   */
  def createRDD[K, V](sc: SparkContext,
                      kafkaParams: ju.Map[String, Object],
                      offsetRanges: Array[OffsetRange],
                      locationStrategy: LocationStrategy): RDD[ConsumerRecord[K, V]] = {
    KafkaUtils.createRDD(sc, kafkaParams, offsetRanges, locationStrategy)
  }


  /**
   * 更新 Offsets
   *
   * @param groupId
   * @param offsets
   */
  def updateOffsets(groupId: String, offsets: Array[OffsetRange]): Unit = {
    val offsetInfos = offsets.map(x => new TopicPartition(x.topic, x.partition) -> x.untilOffset).toMap
    offsetsManager.updateOffsets(groupId, offsetInfos)
  }

}