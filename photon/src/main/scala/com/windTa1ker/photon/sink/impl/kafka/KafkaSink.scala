package com.windTa1ker.photon.sink.impl.kafka

import java.util.{Properties, UUID}

import com.windTa1ker.photon.sink.Sink
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Time
import KafkaWriter._
import scala.collection.JavaConversions._
/**
 * @time: 2020/7/1 4:37 下午
 * @author: 荆旗
 * @Description:
 */
class KafkaSink[T](@transient override val sc : SparkContext,
                   initParams: Map[String,String] = Map.empty[String,String]) extends Sink[T]{

  override val paramPrefix: String = "spark.sink.kafka."

  private lazy val prop = {
    val p = new Properties()
    p.putAll(param ++ initParams)
    p
  }

  private val outputTopic = prop.getProperty("topic")

  /**
   * 以字符串的形式输出到kafka
   *
   */
  override def output(rdd: RDD[T], time: Time = Time(System.currentTimeMillis())): Unit = {
    rdd.writeToKafka(prop, x => new ProducerRecord[String, String](outputTopic, UUID.randomUUID().toString, x.toString))
  }

}
