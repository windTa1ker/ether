package com.windTa1ker.photon

import com.windTa1ker.photon.utils.Utils
import org.apache.spark.streaming.scheduler.StreamingListener
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ArrayBuffer

/**
 * time: 2020/7/1 2:22 下午
 * author: 荆旗
 * Description:
 */
trait PhotonStreaming extends PhotonBase {


  // checkpoint目录
  private var checkpointPath: String = ""

  // 从checkpoint 中恢复失败，则重新创建
  private var createOnError: Boolean = true

  /**
   * StreamingContext 运行之后执行
   */
  def afterStarted(ssc: StreamingContext): Unit = {}

  /**
   * StreamingContext 停止后 程序停止前 执行
   */
  def beforeStop(ssc: StreamingContext): Unit = {}


  /**
   * 处理函数
   *
   * @param ssc
   */
  def handle(ssc: StreamingContext): Unit


  /**
   * 创建 Context
   *
   * @return
   */
  def creatingContext(propFile: String): StreamingContext = {
    val sparkConf = init(propFile)
    val extraListeners = sparkListeners.mkString(",") + "," + sparkConf.get("spark.extraListeners", "")
    if (extraListeners != ",") sparkConf.set("spark.extraListeners", extraListeners)
    // 时间间隔
    val slide = sparkConf.get("spark.batch.duration").toInt
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(slide))
    addStreamingListener(ssc)
    handle(ssc)
    ssc
  }

  def creatingContext(): StreamingContext = creatingContext(null)

  private def addStreamingListener(ssc: StreamingContext): Unit = {
    val listenerList = ssc.sparkContext.getConf.get("spark.streaming.extraListeners", "").split(",")
    val supervisor = classOf[StreamingListener]
    listenerList.filter("" !=).foreach(className => {
      val clazz = Class.forName(className, true, Thread.currentThread().getContextClassLoader)
      if (supervisor.isAssignableFrom(clazz)) {
        val constructor = clazz.getConstructor(classOf[StreamingContext])
        val listener = constructor.newInstance(ssc).asInstanceOf[StreamingListener]
        ssc.addStreamingListener(listener)
      } else throw new Exception("Must be a subclass of StreamingListener")
    })
  }

  def main(args: Array[String]): Unit = {

    val propFile: String = if (args.length > 0) args(0) else null
    val configMap = propFile match {
      case null => null
      case _ => Utils.getPropertiesFromFile(propFile)
    }
    checkpointPath = if(configMap == null) "" else configMap.getOrElse("spark.photon.checkpoint.path", "")
    val context = checkpointPath match {
      case "" => creatingContext(propFile)
      case ck =>
        val ssc = StreamingContext.getOrCreate(ck, creatingContext, createOnError = createOnError)
        ssc.checkpoint(ck)
        ssc
    }

    context.start()
    afterStarted(context)
    context.awaitTermination()
    beforeStop(context)
  }
}
