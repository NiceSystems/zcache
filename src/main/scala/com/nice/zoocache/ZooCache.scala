package com.nice.zoocache

import org.msgpack.ScalaMessagePack._
import collection.JavaConversions._
import org.apache.zookeeper.{WatchedEvent, Watcher}
import com.netflix.curator.framework
import framework.{CuratorFramework, CuratorFrameworkFactory}
import grizzled.slf4j.Logging
import akka.actor.{Props, ActorSystem}
import akka.pattern.ask
import akka.dispatch.{Future, Await}
import akka.util.duration._
import akka.util.{Duration, Timeout}
import java.util.UUID
import PathString._

/**
 * User: arnonrgo
 * Date: 1/15/13
 * Time: 5:13 PM
 */


class ZooCache(connectionString: String,systemId : String, private val useLocalShadow: Boolean = false,private val interval : Duration = 30 minutes, maxWait : Duration =1 second) {

  val cache = ZooCacheSystem.system.actorFor(ZooCacheSystem.system / ZooCacheSystem.CACHE_ID.noPath)

  val atMost=maxWait
  implicit val timeout=Timeout(maxWait)

  val id = Await.result(cache ? Register(systemId,connectionString,useLocalShadow,interval), atMost).asInstanceOf[UUID]

  /**
   * invalidate the local cache on all clients - can be used by any client of the cache
   * for instance you cna add configurations to the cache with a TTL forever so clients will use local copies
   * but invalidate all the clients if the configuration changes anyway
   */
  def invalidate(){
    val systemInvalidationPath=ZooCacheSystem.INVALIDATE_PATH :> systemId
    cache ! Invalidate(id,systemInvalidationPath)
  }

  /**
   * check if a key is in the cache
   *
   * @param key the id (string) of for the value
   */
  def doesExist(key : String) =  cache ? Exists(id,key)


  /**
   * Add a value to the cache with no expiration
   *
   * @param key the id (string) of for the value
   * @param input the object to put in the cache
   */
  def put(key :String, input : Any) : Future[Boolean] = put(key,input,ZooCacheSystem.FOREVER)


  /**
   * Add a value to the cache with a time-to-live (ttl)
   *
   * @param key the id (string) of for the value
   * @param input the object to put in the cache
   * @param ttl time to live in milliseconds
   */
  def put(key :String, input : Any, ttl: Long) : Future[Boolean] = {
    for ( r <-cache ? Put(id,key,pack(input),ttl)) yield r.asInstanceOf[Boolean]
  }



  /**
   * Add a value to the cache with a time-to-live (ttl) under a parent group
   *
   * @param parent the id (string) of the group
   * @param key the id (string) of for the value
   * @param value the object to put in the cache
   * @param ttl time to live in milliseconds
   */
  def put(parent:String,key :String, value : Any, ttl :Long)  : Future[Boolean] = put(parent :> key,value,ttl)


  /**
   * Add a value to the cache with no expiration under a parent group
   *
   * @param parent the id (string) of the group
   * @param key the id (string) of for the value
   * @param value the object to put in the cache
   */
  def put(parent:String,key :String, value : Any) : Future[Boolean] =  put(parent :> key,value)


  /**
   * get a value from the cache
   *
   *
   * @return T - the value retrieved from the cache as an Option
   * @param key the id (string) of for the value
   */
  def get[T<:AnyRef](key: String)(implicit manifest : Manifest[T]):Future[Option[T]] = {
     val futVal=cache ? GetValue(id,key)
     for (f<-futVal)
     yield  f.asInstanceOf[Option[Array[Byte]]] match {
       case Some(value) => Some(unpack[T](value))
       case None => None
     }
  }

  /**
   * get a value from the cache from a group
   *
   * @return T - the value retrieved from the cache, null if not found
   * @param parentKey the id of the group
   * @param key the id (string) of for the value
   */
  def get[T<:AnyRef](parentKey: String,key: String)(implicit manifest : Manifest[T]):Future[Option[T]] =  get[T](parentKey :> key)



  /**
   * get a value from the cache in a Java friendly API
   *
   *
   * @return T - the value retrieved from the cache, null if not found
   * @param key the id (string) of for the value
   * @param jType class of the value (e.g. String.class)
   */
  def get[T >: Null <: AnyRef](key: String, jType: Class[T]) :Future[T]  ={
    for (f<- get[T](key)(Manifest.classType(jType))) yield f.getOrElse(null);
  }

  /**
   * get a value from the cache from a group in a Java friendly API
   *
   * @return T - the value retrieved from the cache, null if not found
   * @param parent the id of the group
   * @param key the id (string) of for the value
   * @param jType class of the value (e.g. String.class)
   */
  def get[T >: Null <: AnyRef](parent: String,key: String, jType: Class[T]) :Future[T]  ={
    for (f<- get[T](parent,key)(Manifest.classType(jType))) yield f.getOrElse(null);
  }


  /**
   * Remove all the items from a group
   *
   * @param key the id (string) of the group  or item to remove
   */
  def removeItem(key: String) {
    cache ! RemoveKey(id,key)
  }


}
