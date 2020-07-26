package com.windTa1ker.algorithm.recursive

/**
 * time: 2020/7/7 9:06 上午
 * author: 荆旗
 * Description: 
 */
object Others {

  def isArraySortedAsc(arr: Array[Int], start: Int, end: Int): Boolean = {
    var flag = false
    if (end - start == 0) { // 有零个元素或者一个元素
      flag = true
    } else if (end - start == 1 && arr(end) > arr(start)) {
      flag = true
    } else {
      arr(start + 1) > arr(start) match {
        case true =>
          flag = isArraySortedAsc(arr, start + 1, end)
        case false =>
      }
    }
    flag
  }

  def main(args: Array[String]): Unit = {
    val arr1 = Array(1, 2, 3, 4, 5)
    val arr2 = Array(1, 2, 7, 4, 5)
    // println(isArraySortedAsc(arr1, 0, arr1.length-1))
    println(isArraySortedAsc(arr2, 0, arr2.length-1))
  }

}
