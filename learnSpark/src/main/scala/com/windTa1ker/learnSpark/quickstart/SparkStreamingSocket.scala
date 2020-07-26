package com.windTa1ker.learnSpark.quickstart

import com.windTa1ker.photon.PhotonStreaming
import com.windTa1ker.photon.source.impl.socket.SocketSource
import org.apache.spark.streaming.StreamingContext

/**
 * time: 2020/7/3 6:23 下午
 * author: 荆旗
 * Description: 
 */
object SparkStreamingSocket extends PhotonStreaming{
  /**
   * 处理函数
   *
   * @param ssc
   */
  override def handle(ssc: StreamingContext): Unit = {
    val source = new SocketSource[String](ssc)
    val dstream = source.getDStream[String](_.toString)
    dstream.foreachRDD( rdd =>
      println(rdd.count())
    )
  }
}
