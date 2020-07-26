package com.windTa1ker.algorithm.linkedlist

/**
 * time: 2020/7/7 12:05 下午
 * author: 荆旗
 * Description: 
 */
class ListNode(val data: Int, var next: ListNode) {
}

object ListNode {

  /**
   * 遍历链表
   *
   * @param head   头结点
   * @param handle 处理函数
   */
  def traverseLinkedList(head: ListNode, handle: ListNode => Unit): Unit = {
    var temp = head;
    while (temp != null) {
      handle(temp)
      temp = temp.next
    }
  }

  /**
   * 链表长度
   *
   * @param node 头结点
   */
  def length(node: ListNode) = {
    var len = 0
    var temp = node
    while (node != null) {
      len = len + 1
      temp = temp.next
    }
    len
  }

  /**
   * 插入节点, 分三种情况
   * 1. 头节点插入
   * 2. 中间结点插入
   * 3. 尾插入
   *
   * @param head
   * @param node2insert
   * @param pos
   * @return
   */
  def insert(head: ListNode, node2insert: ListNode, pos: Int): ListNode = {
    var _head = head
    val len = length(head)
    if (head == null) _head = node2insert
    if (pos > len + 1 || pos < 1) {
      println(s"pos of node to insert not valid, the valid input pos is 1 to ${len + 1}")
    } else if (pos == 1) {
      node2insert.next = head
      _head = node2insert
    } else {
      var count = 2
      var _temp = head
      while (count != pos) {
        _temp = _temp.next
        count = count + 1
      }
      node2insert.next = _temp.next
      _temp.next = node2insert
    }
    _head
  }

  def deleteNode(head: ListNode, pos: Int) = {
    val len = length(head)
    var _head = head
    if (head == null) {
      println("head is null ...... ")
    } else if (pos < 1 || pos > len) {
      println(s"pos of node to insert not valid, the valid input pos is 1 to ${len}")
    } else if (pos == 1) {
      _head = head.next
    } else {
      var temp = head
      var count = 2
      while (count != pos) {
        temp = temp.next
        count = count + 1
      }
      temp.next = temp.next.next
    }
    _head
  }

  def main(args: Array[String]): Unit = {

  }
}
