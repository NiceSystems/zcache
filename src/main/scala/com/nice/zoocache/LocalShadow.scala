package com.nice.zoocache
import org.apache.commons.collections.map.LRUMap
import grizzled.slf4j.Logging
import akka.actor.Actor


case class Remove(k:String)
case class Update(k:String,v:Any)
case class Contains(k:String)
case class Clear()
case class Get(k:String)


/**
 * Simple LRU Cache wrapping {@link org.apache.commons.collections.map.LRUMap}
 *
 * @param size the maximum number of Elements allowed in the LRU map
 */
private class LocalShadow(size: Int) extends Actor with Logging {
  //todo:consider adding a load factor to the LRU initialization
  private val map= new LRUMap(size)

  def receive = {
    case Remove(k) => remove(k)
    case Update(k,v) => update(k,v)
    case Contains(k) => sender ! contains(k)
    case Clear() => clear()
    case Get(k:String) => sender ! getObject(k)
  }
  private def clear(){
    map.clear()
  }



  private def update(k: String, v:Any) {
    map.put(k, v)
  }

  private def remove(k: String) = map.remove(k)

  private def getObject(k: String): Option[Any] = {
    if (!contains(k)) None
    else
      Some(map.get(k))
  }

  private def contains(k: String): Boolean = map.containsKey(k)
}
