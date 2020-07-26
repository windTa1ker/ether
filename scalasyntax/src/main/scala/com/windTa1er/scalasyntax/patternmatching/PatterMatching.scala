package com.windTa1er.scalasyntax.patternmatching

/**
 * time: 2020/7/7 8:25 上午
 * author: 荆旗
 * Description: 
 */
object PatterMatching {


  def betterSwitch(ch: Char) = {
    var sign: Int = -2
    ch match {
      case '+' => sign = 1
      case '-' => sign = -1
      case _ => sign = 0 // 如果没有 _, 将会抛出 MatchError
    }
    println(sign)
  }

  def pmWithOr() = {
    val prefix = ""
    prefix match {
      case "0" | "1" | "2" => println("012")
      case _ => println("other")
    }
  }

  def pmWithGuard(ch: Char): Unit = {
    ch match {
      case _ if Character.isDigit(ch) =>
        val digit = Character.digit(ch, 8)
        println(digit)
      case x => println(x)
    }
  }

  def main(args: Array[String]): Unit = {
    pmWithGuard('9')
  }


}
