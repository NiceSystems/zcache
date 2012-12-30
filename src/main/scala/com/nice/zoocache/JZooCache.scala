package com.nice.zoocache

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
 * @param localCacheSize The size of the shadow copy of the Zookeeper cache (a local LRU cache)
 */
class JZooCache(connectionString: String,systemId : String, localCacheSize : Int){
  lazy val cache= new ZooCache(connectionString,systemId, localCacheSize)


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
  def removeAll(parent : String){
    cache.removeAll(parent)
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
