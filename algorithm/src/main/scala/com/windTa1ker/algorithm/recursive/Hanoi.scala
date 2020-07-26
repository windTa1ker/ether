package com.windTa1ker.algorithm.recursive

/**
 * time: 2020/7/7 7:54 上午
 * author: 荆旗
 * Description:
 * 经典用例：
 *     1. 斐波纳契数列，阶乘
 *     2. 归并排序，快排
 *     3. 二分查找
 *     4. 树的前，中，后序遍历
 *     5. 图的遍历：深搜和广搜
 *     6. 动态规划
 *     7. 分治算法
 *     8. 汉诺塔
 *     9. 回溯算法
 */
object Hanoi {

  def hanoi(n: Int, from: String, to: String, mid: String): Unit = {
    n match {
      case 1 =>
        println(from + " ==> " + to)
      case x =>
        hanoi(n - 1, from, mid, to)
        println(from + " ==> " + to)
        hanoi(n - 1, mid, to, from)
      case x if x <= 0 => throw new Exception("n must be positive...")
    }
  }

  def main(args: Array[String]): Unit = {
    hanoi(3, "A", "B", "C")
  }
}
