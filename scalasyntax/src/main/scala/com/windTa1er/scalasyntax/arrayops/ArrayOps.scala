package com.windTa1er.scalasyntax.arrayops

import scala.collection.JavaConversions._
/**
 * time: 2020/7/7 9:25 上午
 * author: 荆旗
 * Description: 
 */
object ArrayOps {

  val nums = new Array[Int](10)
  nums(5) = 10
  println(nums(5))

  val a = new Array[String](10)
  val s = Array("Hello", "world")
  s(0) = "GoodBye"
  s.foreach(println)
  def main(args: Array[String]): Unit = {

  }

}
