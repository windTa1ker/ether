package com.windTa1ker.photon.source

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.slf4j.{Logger, LoggerFactory}

import scala.annotation.meta.getter
import scala.reflect.ClassTag
import scala.util.Try

/**
 * @time: 2020/7/2 12:18 下午
 * @author: 荆旗
 * @Description:
 */
trait Source extends Serializable {
  lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  @transient @getter
  val ssc: StreamingContext
  @transient @getter
  lazy val sparkConf: SparkConf = ssc.sparkContext.getConf

  val paramPrefix: String

  lazy val param: Map[String, String] = sparkConf.getAll.flatMap {
    case (k, v) if k.startsWith(paramPrefix) && Try(v.nonEmpty).getOrElse(false) =>
      Some(k.substring(paramPrefix.length) -> v)
    case _ => None
  } toMap


  type SourceType

  /**
   * 获取DStream 流
   *
   * @return
   */
  def getDStream[R: ClassTag](f: SourceType => R): DStream[R]
}