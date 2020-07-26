package com.windTa1ker.photon.sink.impl.kafka

import java.util.Properties

import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

/**
 * @time: 2020/7/2 12:12 上午
 * @author: 荆旗
 * @Description:
 */

abstract class KafkaWriter[T] {
  def writeToKafka[K, V](producerConfig: Properties, serializerFunc: T => ProducerRecord[K, V]): Unit
}

object KafkaWriter {
  /**
   * This implicit method allows the user to call dstream.writeToKafka(..)
   *
   * @param dstream - DStream to write to Kafka
   * @tparam T - The type of the DStream
   * @tparam K - The type of the key to serialize to
   * @tparam V - The type of the value to serialize to
   * @return
   */
  implicit def createKafkaOutputWriter[T, K, V](dstream: DStream[T]): KafkaWriter[T] = {
    new DStreamKafkaWriter[T](dstream)
  }

  implicit def createKafkaOutputWriter[T, K, V](rdd: RDD[T]): KafkaWriter[T] = {
    new RDDKafkaWriter[T](rdd)
  }

}