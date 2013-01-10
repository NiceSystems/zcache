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
import org.msgpack.{ScalaMessagePack, MessagePack}
import javax.xml.soap.SOAPMessage

/**
 * User: arnonrgo
 * Date: 12/30/12
 * Time: 2:25 PM
 */


/**
 * Clean Java friendly API (no Scala niceties that look odd in Java)
 *
 * @param connectionString Zookeeper quorum connection string
 * @param systemId unique Id for the cache (provides a path in Zookeeper just for the app)
 * @param useLocalShadow Should the cache keep a local copy of values retrieved (a local LRU cache)
 */
class JZooCache(connectionString: String,systemId : String, useLocalShadow : Boolean){
  lazy val cache= new ZooCache(connectionString,systemId, useLocalShadow)


  /**
   * Add a value to the cache with no expiration (can still be invalidated un-masse with all the local copies)
   *
   * @param key the id (string) of for the value
   * @param value the object to put in the cache
   */
  def put(key: String, value: Any) : Boolean= {
    cache.put(key,value,ZooCache.FOREVER)
  }

  /**
   * Add a value to the cache with a time-to-live (ttl)
   *
   * @param key the id (string) of for the value
   * @param value the object to put in the cache
   * @param ttl time to live in miliseconds
   */
  def put(key: String, value: Any,ttl : Long) : Boolean= {
    cache.put(key,value,ttl)
  }

  /**
   * Add a value to the cache with a time-to-live (ttl) under a parent group
   *
   * @param parent the id (string) of the group
   * @param key the id (string) of for the value
   * @param value the object to put in the cache
   * @param ttl time to live in miliseconds
   */
  def put(parent: String, key : String, value: Any,ttl : Long): Boolean={
    cache.put(parent,key,value,ttl)
  }

  /**
   * Remove all the items from a group
   *
   * @param parent the id (string) of the group to remove
   */
  def removeItem(parent : String){
    cache.removeItem(parent)
  }

  /**
   * invalidate the local cache on all clients - can be used by any client of the cache
   * for instance you cna add configurations to the cache with a TTL forever so clients will use local copies
   * but invalidate all the clients if the configuration changes anyway
   */
  def invalidate() {
    cache.invalidate()
  }

  /**
   * get a value from the cache
   *
   *
   * @return T - the value retrieved from the cache, null if not found
   * @param key the id (string) of for the value
   * @param jType class of the vlaue (e.g. String.class)
   */
  def get[T >: Null <: AnyRef](key: String, jType: Class[T]) : T ={
    (cache.get[T](key)(Manifest.classType(jType))).getOrElse(null)
  }

  /**
   * get a value from the cache from a group
   *
   * @return T - the value retrieved from the cache, null if not found
   * @param parent the id of the group
   * @param key the id (string) of for the value
   * @param jType class of the vlaue (e.g. String.class)
   */
  def get[T >: Null <: AnyRef](parent: String,key: String, jType: Class[T]) : T ={
    (cache.get[T](parent,key)(Manifest.classType(jType))).getOrElse(null)
  }

}
