package com.windTa1ker.photon.sink.impl.kafka

import java.util.Properties

import org.apache.kafka.clients.producer.KafkaProducer

import scala.collection.mutable

/**
 * @time: 2020/7/2 12:16 上午
 * @author: 荆旗
 * @Description:
 */
object ProducerCache {
  private val producers = new mutable.HashMap[Properties, Any]()
  def getProducer[K, V](config: Properties): KafkaProducer[K, V] = {
    producers.getOrElse(config, {
      val producer = new KafkaProducer[K, V](config)
      producers(config) = producer
      producer
    }).asInstanceOf[KafkaProducer[K, V]]
  }
}
