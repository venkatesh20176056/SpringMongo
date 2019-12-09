package com.paytm.map.features.utils

import scala.collection.immutable.TreeSet

class BoundedPriorityQueue[A](val ts: TreeSet[A], val bound: Int, val lowest: Option[A] = None, val size: Int = 0)(implicit val ordering: Ordering[A]) extends Serializable {

  def insert(elem: A): BoundedPriorityQueue[A] = {
    if (ts.contains(elem)) this
    else if (lowest.isEmpty) new BoundedPriorityQueue(ts + elem, bound, Some(elem), 1)
    else if (size < bound) new BoundedPriorityQueue(ts + elem, bound, resolveMin(elem), size + 1)
    else possiblyReplaceLowest(elem)
  }

  private[utils] def resolveMin(elem: A): Option[A] = {
    if (lowest.isDefined && ordering.lt(lowest.get, elem)) Some(elem)
    else lowest
  }

  def union(bpq: BoundedPriorityQueue[A]): BoundedPriorityQueue[A] = {
    (bpq.ts &~ ts).foldLeft(this)((newBpq: BoundedPriorityQueue[A], elem: A) => newBpq.insert(elem))
  }

  private[utils] def possiblyReplaceLowest(elem: A): BoundedPriorityQueue[A] = {
    if (lowest.isDefined && ordering.lt(elem, lowest.get)) new BoundedPriorityQueue[A](ts.dropRight(1) + elem, bound, Some(elem), size)
    else this
  }

  override def toString: String = s"BoundedPriorityQueue(${ts.mkString(",")})"

  override def equals(other: Any): Boolean = {
    other match {
      case b: BoundedPriorityQueue[A] =>
        this.ts == b.ts &&
          this.bound == b.bound &&
          this.lowest == b.lowest &&
          this.size == b.size
      case _ => false
    }
  }

}
