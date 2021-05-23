package com.windTa1ker.algorithm.ch2.backtracking

/**
 * time: 2020/7/7 9:47 上午
 * author: 荆旗
 * Description:
 * 回溯算法经典用例：
 *     1. 产生二进制串
 *     2. 生成 k 进制串
 *     3. 背包问题
 *     4. 广义字符串
 *     5. 哈密顿回路问题
 *     6. 图着色问题
 */
object BackTracking {

  /**
   * 问题: 生成 二进制串
   * 将解空间想象成一颗树
   *     root
   *   0     1
   * 0   1 0   1
   * @param arr 用于放二进制的串
   * @param level 第几层
   */
  def binaryString(arr: Array[Int], level: Int): Unit = {
    if(level <= arr.length - 1){
      arr(level) = 0
      binaryString(arr, level + 1)
      arr(level) = 1
      binaryString(arr, level + 1)
    }else{
      arr.foreach(print)
      println()
    }
  }

  /**
   * 生成 k 进制字符串
   * 想法同上
   * @param arr 用来存放所有数字的数组
   *
   * @param level 层级， 也是索引
   * @param k 进制
   */
  def kString(arr: Array[Int], level: Int, k: Int): Unit = {
    if(level <= arr.length - 1){
      for(x <- 0 until k){
        arr(level) = x
        kString(arr, level + 1, k)
      }
    }else{
      arr.foreach(print)
      println()
    }
  }

  def main(args: Array[String]): Unit = {
    val arr = new Array[Int](3)
    binaryString(arr, 0)

    println("---------------------------")

    val arr2 = new Array[Int](3)
    kString(arr2, 0, 2)
  }
}
