package com.nice.zoocache


import com.netflix.curator.retry.ExponentialBackoffRetry

import org.msgpack.ScalaMessagePack._
import collection.JavaConversions._
import org.apache.zookeeper.{WatchedEvent, Watcher}
import java.util.Date
import com.netflix.curator.framework
import framework.{CuratorFramework, CuratorFrameworkFactory}
import grizzled.slf4j.Logging


/**
 * User: arnonrgo
 * Date: 12/26/12
 * Time: 10:47 AM
 */

object ZooCache{
    val FOREVER = -2
    private val TTL_PATH = "/ttl"
    private val CACHE_ROOT = "/cache/"
    private val MAX_LOCAL_SHADOW_SIZE = 20000
    private val INVALIDATE_PATH="/invalidate/"
}
class ZooCache(connectionString: String,systemId : String, useLocalShadow : Boolean = false) extends Logging {
  private val retryPolicy = new ExponentialBackoffRetry(1000, 10)

  private var client : CuratorFramework  = null
  private var localInvalidationClient: CuratorFramework = null

  buildClients()

  def buildClients() {
    debug("(re)building clients")
    client = CuratorFrameworkFactory.builder().
      connectString(connectionString).
      namespace(ZooCache.CACHE_ROOT+systemId).
      retryPolicy(retryPolicy).
      build

    client.start()

    if (useLocalShadow) {

      localInvalidationClient =  CuratorFrameworkFactory.builder().
        connectString(connectionString).
        namespace(ZooCache.INVALIDATE_PATH+systemId).
        retryPolicy(retryPolicy).
        build

      localInvalidationClient.start()
    }
  }


  lazy private val shadow = new LocalShadow(ZooCache.MAX_LOCAL_SHADOW_SIZE)

  def doesExist(key : String) : Boolean =  if (client.checkExists().forPath("/"+key)!=null) true else false


  def removeAll(parentKey: String) {
    val path="/"+parentKey
    val children=client.getChildren.forPath(path)


    //todo: consider making a recursive function
    for (child <- children) {
      for(grandchild <-client.getChildren.forPath(path+"/"+child)) client.delete().forPath(path+"/"+child+"/"+grandchild)
      client.delete().forPath(path+"/"+child)
    }
    client.delete().inBackground().forPath(path)
  }

  private[zoocache] def putBytes (key : String, input :Array[Byte],ttl: Array[Byte]):Boolean  ={
    val path="/"+key
    val ttlPath=path+ZooCache.TTL_PATH

    try {
      def ensurePath(path:String) {
        val ensurePath = client.newNamespaceAwareEnsurePath(path)
        ensurePath.ensure(client.getZookeeperClient)
      }
      ensurePath(path)
      ensurePath(ttlPath)

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
    def getFromZoo(path :String): Option[Array[Byte]] = {
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

    val path="/"+key
    var result= if (useLocalShadow) shadow.get[Array[Byte]](path) else None
    result = if (result==None) getFromZoo(path) else result

    //todo: move shadow writes to external get
    if (useLocalShadow && result!=None) shadow.update(path,result.get)

    result

  }

  def put(key :String, input : Any, ttl: Long = ZooCache.FOREVER):Boolean ={
    //todo: add transaction for both writes
    val meta=new ItemMetadata()
    meta.ttl=ttl
    val wasSuccessful=putBytes(key,pack(input),pack(meta))

    if (wasSuccessful && useLocalShadow) {
     shadow.update(key+ZooCache.TTL_PATH,meta)
     shadow.update(key,input)
    }

    wasSuccessful
  }


  def get[T](key: String)(implicit manifest : Manifest[T]):Option[T] = {
    // validate time to live
    val metaBytes=getBytes(key+ZooCache.TTL_PATH)
    metaBytes match {
      case Some(meta) => {
        val current=new Date().getTime
        val expiration = unpack[ItemMetadata](meta).expirationTime
        if(current > expiration) return None
      }
      case None =>return None
    }

    val data = getBytes(key)
    data match {
      case Some(result) =>  Some(unpack[T](result))
      case None => None  // key not found
    }
  }

  def get[T](parentKey: String,key: String)(implicit manifest : Manifest[T]):Option[T] = {
    get[T](parentKey+"/"+key)
  }

  def put(parentKey:String,key :String, input : Any):Boolean ={
    put(parentKey+"/"+key,input)
  }
  def put(parentKey:String,key :String, input : Any, ttl :Long):Boolean ={
    put(parentKey+"/"+key,input,ttl)
  }


}
