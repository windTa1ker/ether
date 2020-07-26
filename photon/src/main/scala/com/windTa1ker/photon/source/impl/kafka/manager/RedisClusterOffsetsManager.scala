package com.windTa1ker.photon.source.impl.kafka.manager

import com.windTa1ker.photon.utils.redis.{RedisClusterUtil, RedisEndpoint}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import redis.clients.jedis.Protocol
import scala.collection.JavaConversions._
import scala.util.Try

/**
 * @time: 2020/7/2 12:40 下午
 * @author: 荆旗
 * @Description:
 */
class RedisClusterOffsetsManager(val sparkConf: SparkConf) extends OffsetsManager {

  lazy val hosts: String = storeParams.getOrElse("redis.hosts", Protocol.DEFAULT_HOST).trim
  lazy val port: Int = storeParams.getOrElse("redis.port", Protocol.DEFAULT_PORT.toString).toInt
  lazy val auth: String = Try(storeParams("redis.auth")) getOrElse null
  lazy val dbNum: Int = storeParams.getOrElse("redis.dbnum", Protocol.DEFAULT_DATABASE.toString).toInt
  lazy val timeout: Int = storeParams.getOrElse("redis.timeout", Protocol.DEFAULT_TIMEOUT.toString).toInt

  lazy val redisEndpoint = RedisEndpoint(hosts, port, auth, dbNum, timeout)

  private val jedis = RedisClusterUtil.connect(redisEndpoint)

  override def getOffsets(groupId: String, topics: Set[String]): Map[TopicPartition, Long] = {
    val offsets = topics.flatMap(topic => {
      jedis.hgetAll(generateKey(groupId,topic)).map {
        case (partition,offset) => new TopicPartition(topic,partition.toInt) -> offset.toLong
      }
    })
    offsets.toMap
  }


  override def updateOffsets(groupId: String, offsetInfos: Map[TopicPartition, Long]): Unit = {
    offsetInfos.foreach { case (tp, offset) =>
      jedis.hset(generateKey(groupId, tp.topic), tp.partition().toString, offset.toString)
    }
  }

  override def delOffsets(groupId: String, topics: Set[String]): Unit = {
    topics.foreach(x => jedis.del(generateKey(groupId, x)))
  }
}