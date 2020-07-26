package com.windTa1ker.photon.source.impl.kafka.manager

import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import scala.collection.JavaConversions._
import com.windTa1ker.photon.utils.redis.RedisConnectionPool._
/**
 * @time: 2020/7/2 12:35 下午
 * @author: 荆旗
 * @Description:
 */
class RedisOffsetsManager(val sparkConf: SparkConf) extends OffsetsManager {

  override def getOffsets(groupId: String, topics: Set[String]): Map[TopicPartition, Long] = {
    val offsets = safeClose {jedis =>
      topics.flatMap(topic => {
        jedis.hgetAll(generateKey(groupId,topic)).map {
          case (partition,offset) => new TopicPartition(topic,partition.toInt) -> offset.toLong
        }
      })
    }(connect(storeParams))
    offsets.toMap
  }


  override def updateOffsets(groupId: String, offsetInfos: Map[TopicPartition, Long]): Unit = {
    safeClose { jedis =>
      for ((tp, offset) <- offsetInfos) {
        jedis.hset(generateKey(groupId, tp.topic), tp.partition().toString, offset.toString)
      }
    }(connect(storeParams))
  }

  override def delOffsets(groupId: String, topics: Set[String]): Unit = {
    safeClose { jedis =>
      for (topic <- topics) {
        jedis.del(generateKey(groupId, topic))
      }
    }(connect(storeParams))
  }
}