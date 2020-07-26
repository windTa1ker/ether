package com.windTa1ker.photon.sink.impl.kafka

import java.util.Properties

import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.spark.streaming.dstream.DStream

import scala.annotation.meta.param

/**
 * @time: 2020/7/2 12:28 上午
 * @author: 荆旗
 * @Description:
 */
class DStreamKafkaWriter [T](@(transient @param) dstream: DStream[T]) extends KafkaWriter[T] {

  /**
   *
   * @param producerConfig The configuration that can be used to connect to Kafka
   * @param serializerFunc The function to convert the data from the stream into Kafka
   *                       [[ProducerRecord]]s.
   * @tparam K The type of the key
   * @tparam V The type of the value
   *
   */
  override def writeToKafka[K, V](producerConfig: Properties,
                                  serializerFunc: T => ProducerRecord[K, V]): Unit = {
    dstream.foreachRDD { rdd =>
      val rddWriter = new RDDKafkaWriter[T](rdd)
      rddWriter.writeToKafka(producerConfig, serializerFunc)
    }
  }
}