package com.windTa1ker.photon.source.impl.kafka

import java.util.concurrent.ConcurrentHashMap

import com.windTa1ker.photon.source.Source
import com.windTa1ker.photon.source.impl.kafka.manager.KafkaManager
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, OffsetRange}

import scala.reflect.ClassTag

/**
 * @time: 2020/7/2 12:19 下午
 * @author: 荆旗
 * @Description:
 */
class KafkaDirectSource[K, V](@transient val ssc: StreamingContext,
                              specialKafkaParams: Map[String, String] = Map.empty[String, String]) extends Source {

  override val paramPrefix: String = "spark.source.kafka.consumer."
  // 保存 offset
  private lazy val offsetRanges: java.util.Map[Long, Array[OffsetRange]] = new ConcurrentHashMap[Long, Array[OffsetRange]]
  private var canCommitOffsets: CanCommitOffsets = _
  // 分区数
  private lazy val repartition: Int = sparkConf.get(s"$paramPrefix.repartition", "0").toInt
  // 组装 Kafka 参数
  private lazy val kafkaParams: Map[String, String] = param ++ specialKafkaParams
  // kafka 消费 topic
  private lazy val topicSet: Set[String] = kafkaParams("topics").split(",").map(_.trim).toSet

  private lazy val groupId = kafkaParams.get("group.id")

  val km = new KafkaManager(ssc.sparkContext.getConf)

  override type SourceType = ConsumerRecord[K, V]

  /**
   * 获取DStream 流
   *
   * @return
   */
  override def getDStream[R: ClassTag](messageHandler: ConsumerRecord[K, V] => R): DStream[R] = {
    val kp = kafkaParams ++ Map("enable.auto.commit" -> "false")
    val stream = km.createDirectStream[K, V](ssc, kp, topicSet)
    canCommitOffsets = stream.asInstanceOf[CanCommitOffsets]
    stream.transform((rdd, time) => {
      offsetRanges.put(time.milliseconds, rdd.asInstanceOf[HasOffsetRanges].offsetRanges)
      rdd
    }).map(messageHandler)
  }

  /**
   * 更新Offset 操作 一定要放在所有逻辑代码的最后
   * 这样才能保证,只有action执行成功后才更新offset
   */
  def updateOffsets(time: Long): Unit = {
    // 更新 offset
    if (groupId.isDefined) {
      val offset = offsetRanges.get(time)
      km.offsetManagerType match {
        case "kafka" => canCommitOffsets.commitAsync(offset)
        case _ =>
          km.updateOffsets(groupId.get, offset)
      }
    }
    offsetRanges.remove(time)
  }
}