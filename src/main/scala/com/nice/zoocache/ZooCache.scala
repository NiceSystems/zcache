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

import com.netflix.curator.retry.ExponentialBackoffRetry

import org.msgpack.ScalaMessagePack._
import collection.JavaConversions._
import org.apache.zookeeper.{WatchedEvent, Watcher}
import com.netflix.curator.framework
import framework.{CuratorFramework, CuratorFrameworkFactory}
import grizzled.slf4j.Logging
import akka.actor.{Props, ActorSystem}
import akka.pattern.ask
import akka.dispatch.Await
import akka.util.duration._
import akka.util.{Duration, Timeout}
import java.util.UUID

/**
 * User: arnonrgo
 * Date: 12/26/12
 * Time: 10:47 AM
 */


object ZooCache  {


  val FOREVER : Long= -2
  private[zoocache] val TTL_PATH = "/ttl"
  private val CACHE_ID = "cache"
  private[zoocache] val CACHE_ROOT = "/"+CACHE_ID
  private[zoocache] val LOCALSHADOW =  "LocalShadow"
  private[zoocache] val SCAVENGER = "Scavenger"
  private[zoocache] val INVALIDATE_PATH=CACHE_ROOT+"/invalidate"
  private val DEFAULT_LOCAL_CACHE_SIZE = 10000


  private[zoocache] val system=ActorSystem(CACHE_ID)
  private val scavenger=system.actorOf(Props(new Scavenger),name=SCAVENGER)
  private val shadowActor=system.actorOf(Props(new LocalShadow(DEFAULT_LOCAL_CACHE_SIZE)), name =LOCALSHADOW)
  private val cache=system.actorOf(Props(new ZooCacheActor()))


}

class ZooCache(connectionString: String,systemId : String, private val useLocalShadow: Boolean = false,private val interval : Duration = 30 minutes, maxWait : Duration =1 second) extends ZCache with Logging {

  val atMost=maxWait
  implicit val timeout=Timeout(maxWait)

  val id = Await.result(ZooCache.cache ? Register(systemId,connectionString,useLocalShadow,interval), atMost).asInstanceOf[UUID]


  def invalidate(){
     val systemInvalidationPath=ZooCache.INVALIDATE_PATH+"/"+systemId
     ZooCache.cache ! Invalidate(id,systemInvalidationPath)
  }

  def doesExist(key : String) : Boolean =  {
    Await.result(ZooCache.cache ? Exists(id,key),atMost).asInstanceOf[Boolean]
  }


  def put(key :String, input : Any, ttl: Long = ZooCache.FOREVER):Boolean ={
    Await.result(ZooCache.cache ? Put(id,key,pack(input),ttl), atMost).asInstanceOf[Boolean]

  }

  def get[T<:AnyRef](key: String)(implicit manifest : Manifest[T]):Option[T] = {
    Await.result(ZooCache.cache ? GetValue(id,key), atMost).asInstanceOf[Option[Array[Byte]]] match {
      case Some(result)=> Some(unpack[T](result))
      case None => None
    }
  }
  def get[T<:AnyRef](parentKey: String,key: String)(implicit manifest : Manifest[T]):Option[T] = {
    get[T](parentKey+"/"+key)
  }

  def put(parentKey:String,key :String, input : Any):Boolean ={
    put(parentKey+"/"+key,input)
  }
  def put(parentKey:String,key :String, input : Any, ttl :Long):Boolean ={
    put(parentKey+"/"+key,input,ttl)
  }

  def removeItem(key: String) {
    ZooCache.cache ! RemoveKey(id,key)
  }


}
