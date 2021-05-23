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

  @scala.annotation.tailrec
  def isArraySortedInSortedOrder(lst: List[Int]): Boolean = {
    lst.size match {
      case 0 | 1 => true
      case 2 => lst.head < lst(1)
      case _ => if(lst.head < lst(1)) isArraySortedInSortedOrder(lst.tail) else false
    }
  }

  def main(args: Array[String]): Unit = {

    val lst1 = List(1,2,3,4,5)
    val lst2 = List(1,2,5,3,4)
    println(isArraySortedInSortedOrder(lst1))
    println(isArraySortedInSortedOrder(lst2))

  }

}
