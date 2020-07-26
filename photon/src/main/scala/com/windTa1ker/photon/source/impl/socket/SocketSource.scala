package com.windTa1ker.photon.source.impl.socket

import com.windTa1ker.photon.source.Source
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

import scala.reflect.ClassTag

/**
 * time: 2020/7/3 6:05 下午
 * author: 荆旗
 * Description: 
 */
class SocketSource[T](@transient val ssc: StreamingContext,
                           params: Map[String, String] = Map.empty[String, String]) extends Source {
  override val paramPrefix = "spark.source.socket."
  override type SourceType = String

  val ip: String = param.getOrElse(s"${paramPrefix}ip", "localhost")
  val port: String = param.getOrElse(s"${paramPrefix}port", "9999")

  override def getDStream[R: ClassTag](f: SourceType => R): DStream[R] = {
    ssc.socketTextStream(ip, port.toInt)
  }.map(f)

}
