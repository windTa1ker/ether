package com.windTa1er.scalasyntax.log4j

import org.apache.log4j.helpers.Loader
import scala.collection.JavaConversions._


/**
 * time: 2020/9/11 12:02 上午
 * author: 荆旗
 * Description: 
 */
object GetPropsFile {

  def main(args: Array[String]): Unit = {
//    System.getProperties.stringPropertyNames().filter(_ contains("class")).foreach(println)
    System.getProperties.getProperty("java.class.path").split(":").foreach(println)
    println(System.getProperties.getProperty("java.class.path"))
    val file: String = Loader.getResource("log4j.properties").getFile
    println(file)
  }

}
