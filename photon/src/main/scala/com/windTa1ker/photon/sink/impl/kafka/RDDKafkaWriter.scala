package com.windTa1ker.photon.sink.impl.kafka

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.rdd.RDD

import scala.annotation.meta.param

/**
 * @time: 2020/7/2 12:25 上午
 * @author: 荆旗
 * @Description:
 */
class RDDKafkaWriter [T](@transient val rdd: RDD[T]) extends KafkaWriter[T] {

  /**
   *
   * @param producerConfig The configuration that can be used to connect to Kafka
   * @param serializerFunc The function to convert the data from the stream into Kafka
   *                       [[org.apache.kafka.clients.producer.ProducerRecord]]s.
   * @tparam K The type of the key
   * @tparam V The type of the value
   *
   */
  override def writeToKafka[K, V](producerConfig: Properties, serializerFunc: T => ProducerRecord[K, V]): Unit = {
    rdd.foreachPartition(events => {
      val producer: KafkaProducer[K, V] = ProducerCache.getProducer(producerConfig)
      events.map(serializerFunc).foreach(x => producer.send(x).get())
    })
  }
}