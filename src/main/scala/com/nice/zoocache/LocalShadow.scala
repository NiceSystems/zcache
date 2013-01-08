package com.nice.zoocache

/**
 * Copyright (C) 2012 NICE Systems ltd.
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * @author Arnon Rotem-Gal-Oz
 * @version %I%, %G%
 *          <p/>
 */
import org.apache.commons.collections.map.LRUMap
import grizzled.slf4j.Logging
import akka.actor.Actor


case class RemoveLocal(k:String)
case class UpdateLocal(k:String,v:Any)
case class HasLocalCopy(k:String)
case class ClearMemory()
case class GetLocal(k:String)


/**
 * Simple LRU Cache wrapping {@link org.apache.commons.collections.map.LRUMap}
 *
 * @param size the maximum number of Elements allowed in the LRU map
 */
private class LocalShadow(size: Int) extends Actor with Logging {
  //todo:consider adding a load factor to the LRU initialization
  private val map= new LRUMap(size)

  def receive = {
    case RemoveLocal(k) => remove(k)
    case UpdateLocal(k,v) => update(k,v)
    case HasLocalCopy(k) => sender ! contains(k)
    case ClearMemory() => clear()
    case GetLocal(k:String) => sender ! getObject(k)
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
