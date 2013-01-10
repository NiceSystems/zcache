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

/**
 * User: arnonrgo
 * Date: 12/26/12
 * Time: 10:47 AM
 */

//todo: API to retrieve Metadata only
//todo: change ZooCahce interface to return Future instead of Option (possibly unite java and scala interfaces)
//todo: api to invalidate specific items
//todo: consider replacing curator with util-zk (at least for the simple access stuff) ??
//todo: add ACL support in the API
//todo: add multitenancy support
//todo move scavenger interval setting to the scavenger so it can be synchronized across the system
//todo: refactor so that would be one zoocache actor and one localShadow

object ZooCache  {
    val FOREVER : Long= -2
    private[zoocache] val TTL_PATH = "/ttl"
    private val CACHE_ID = "cache"
    private[zoocache] val CACHE_ROOT = "/"+CACHE_ID
    private[zoocache] val LOCALSHADOW =  "LocalShadow"

  private val INVALIDATE_PATH=CACHE_ROOT+"/invalidate"
    private val DEFAULT_LOCAL_CACHE_SIZE = 10000


    private[zoocache] val system=ActorSystem(CACHE_ID)
    private val scavenger=system.actorOf(Props(new Scavenger))
    private val shadowActor=ZooCache.system.actorOf(Props(new LocalShadow(DEFAULT_LOCAL_CACHE_SIZE)), name =LOCALSHADOW)

}

class ZooCache(connectionString: String,systemId : String, private val localCacheSize: Int =1,private val interval : Duration = 30 minutes) extends ZCache with Logging {


  private val retryPolicy = new ExponentialBackoffRetry(1000, 10)
  private val client = CuratorFrameworkFactory.builder().
    connectString(connectionString).
    retryPolicy(retryPolicy).
    build
  client.start()

  initScavenger

  private val useLocalShadow = localCacheSize>1
  if (useLocalShadow) {

    ensurePath(client,systemInvalidationPath)
    client.getChildren.usingWatcher(watcher).forPath(systemInvalidationPath)
  }
  lazy private val systemInvalidationPath=ZooCache.INVALIDATE_PATH +"/"+systemId
  private val basePath=ZooCache.CACHE_ROOT+"/"+systemId+"/"
  implicit val timeout = Timeout(1 second)
  lazy private val watcher : Watcher = new Watcher() {
    override def process(event: WatchedEvent) {
      try {
        //reset the watch as they are one-time
        client.getChildren.usingWatcher(watcher).forPath(systemInvalidationPath)
        //shadow.clear()
        ZooCache.shadowActor ! ClearMemory()
      } catch {
        case e: InterruptedException =>  error("problem processing invalidation event",e)
      }
    }
  }

  def initScavenger {

   val sched=ZooCache.system.scheduler.schedule(0 seconds,interval, ZooCache.scavenger, Tick(systemId,client))
  }



  private def ensurePath(cl:CuratorFramework, path:String) {
    val ensurePath = cl.newNamespaceAwareEnsurePath(path)
    ensurePath.ensure(cl.getZookeeperClient)
  }

  //todo:add invalidate by id
  def invalidate(){
    if (client.checkExists.forPath(systemInvalidationPath+"/doit")==null)
      client.create().forPath(systemInvalidationPath+"/doit")
    else
      client.delete().forPath(systemInvalidationPath+"/doit")
  }

  def doesExist(key : String) : Boolean =  if (client.checkExists().forPath(basePath+key)!=null) true else false



  private[zoocache] def putBytes (key : String, input :Array[Byte],ttl: Array[Byte]):Boolean  ={
    val path=basePath+key
    val ttlPath=path+ZooCache.TTL_PATH

    try {

      ensurePath(client,path)
      ensurePath(client,ttlPath)

      client.inTransaction().
          setData().forPath(path,input).
        and().
          setData().forPath(ttlPath,ttl).
        and().
          commit()

      true
    } catch {
      case e: Exception => {
        error("can't read '"+key+"' from Zookeeper",e)
        false
      }
    }
  }


  private[zoocache] def  getBytes(key:String):Option[Array[Byte]] ={
    val path=basePath+key
    try {
      if (client.checkExists().forPath(path) == null) None
      else
        Some(client.getData.forPath(path))
    } catch {
      case e: Exception => {
        error("can't update '"+ key+"' in Zookeeper",e)
        None
      }
    }

  }

  def put(key :String, input : Any, ttl: Long = ZooCache.FOREVER):Boolean ={
    val meta=new ItemMetadata()
    meta.ttl= ttl
    val wasSuccessful=putBytes(key,pack(input),pack(meta))

    if (wasSuccessful && useLocalShadow) {
          putLocalCopy(key, input, meta)
    }

    wasSuccessful
  }


  private def putLocalCopy(key: String, input: Any, meta: ItemMetadata) {
    ZooCache.shadowActor ! UpdateLocal(key,input)
    ZooCache.shadowActor ! UpdateLocal(key + ZooCache.TTL_PATH, meta)
  }



  def get[T<:AnyRef](key: String)(implicit manifest : Manifest[T]):Option[T] = {

    def isInShadow:Boolean ={
      if (!useLocalShadow) return false
      val reply=Await.result(ZooCache.shadowActor ? GetLocal(key+ZooCache.TTL_PATH), 1 second).asInstanceOf[Option[ItemMetadata]]
     reply match
     {
        case Some(metadata)=>  metadata.isValid
        case None =>  false
      }
    }

    def isInCache:Option[ItemMetadata] ={
      getBytes(key+ZooCache.TTL_PATH) match {
        case Some(meta) => {
                val result=unpack[ItemMetadata](meta)
                if (result.isValid) Some(result) else None
        }
        case None =>None
        }
    }

    def getData:Option[T]={
      val data = getBytes(key)
      data match {
      case Some(result) =>  Some(unpack[T](result))
      case None => None  // key not found
       }
    }

    if (isInShadow) return  Await.result(ZooCache.shadowActor ? GetLocal(key), 1 second).asInstanceOf[Option[T]]

    isInCache match{
      case None => None
      case Some(meta) => {
        val result=getData
        if (useLocalShadow) putLocalCopy(key,result.get,meta)
        result
      }
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
     remove(basePath+key)

    def remove(path :String){
      val children=client.getChildren.forPath(path)

      for (child <- children) {
         removeItem(key+"/"+child)
       }
      client.delete().forPath(path)
      if(useLocalShadow)  ZooCache.shadowActor ! RemoveLocal(key)
  }

  }

  def shutdown(){
    client.close()
  }
}
