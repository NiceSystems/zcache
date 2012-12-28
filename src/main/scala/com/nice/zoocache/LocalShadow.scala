package com.nice.zoocache


import org.apache.commons.collections.map.{LRUMap}
import grizzled.slf4j.Logging

/**
 * Simple LRU Cache wrapping {@link org.apache.commons.collections.map.LRUMap}
 *
 * @param size the maximum number of Elements allowed in the LRU map
 */
class LocalShadow(size: Int) extends Logging {
  // Alternate constructor that gives you no load factor.

  private val map= new LRUMap(size)

  def update(k: String, v:Any) {
    map.put(k, v)
  }

  def remove(k: String) = map.remove(k)

  def get[T](k: String): Option[T] = {
    if (!contains(k)) None
    else
      Some(map.get(k).asInstanceOf[T])
  }
  def contains(k: String): Boolean = map.containsKey(k)
}
