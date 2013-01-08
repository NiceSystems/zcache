package com.nice.zoocache

import akka.util.{Timeout, Duration}
import grizzled.slf4j.Logging
import com.netflix.curator.retry.ExponentialBackoffRetry
import com.netflix.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import akka.actor.{ActorRef, Actor, Props}
import org.apache.zookeeper.{WatchedEvent, Watcher}
import org.msgpack.ScalaMessagePack._

import scala.Some
import akka.dispatch.Await
import akka.util.duration._
import akka.pattern.ask
import java.util.UUID
import collection.JavaConversions._



/**
 * User: arnonrgo
 * Date: 1/6/13
 * Time: 9:54 PM
 */


case class Put(instance : UUID, key: String, value : Array[Byte], ttl: Long)
case class Invalidate(instance : UUID,path: String)
case class GetValue(instance : UUID,key: String)
case class RemoveKey(instance : UUID,key: String)
case class Shutdown()
case class Exists(instance : UUID, key: String)
case class Register(basePath : String, zookeeperConnection : String,useLocalShadow : Boolean)

class ZooCacheActor extends Actor with Logging {

  implicit val timeout = Timeout(1 second)

  val shadowActor:ActorRef = context.actorFor("../LocalShadow")
  val retryPolicy = new ExponentialBackoffRetry(1000, 10)

  var connections = Map[String,CuratorFramework]()
  var registration = Map[UUID,(String, String, Boolean)]()


    def receive ={
      case Put(instance,key,value,ttl) =>  put(instance,key,value,ttl)
      case Invalidate(instance,path) => invalidate(instance,path)
      case GetValue(instance,key)=> sender ! get(instance,key)
      case RemoveKey(instance,key)=> removeItem(instance,key)
      case Exists(instance,key) => sender ! doesExist(instance,key)
      case Shutdown() =>  connections.values.foreach(_.close())
      case Register(basePath,connection,useLocalShadow) => sender ! register(basePath,connection,useLocalShadow)

    }

  private def register(basePath : String, zookeeperConnection : String,useLocalShadow : Boolean) : UUID ={
    val id =UUID.randomUUID()
    val path=ZooCache.CACHE_ROOT+"/"+basePath+"/"
    registration = registration+ (id -> (path,zookeeperConnection,useLocalShadow))
    id
  }
  private def getConnection(instance : UUID): CuratorFramework = {

    val (_,connectionString,_) = registration(instance)

    def establishConnection= {
        val newConn = CuratorFrameworkFactory.builder().
          connectString(connectionString).
          retryPolicy(retryPolicy).
          build
        newConn.start()
        connections= connections + (connectionString->newConn)
        newConn
      }

    connections.getOrElse(connectionString,establishConnection)
  }

  private def ensurePath(cl:CuratorFramework, path:String) {
    val ensurePath = cl.newNamespaceAwareEnsurePath(path)
    ensurePath.ensure(cl.getZookeeperClient)
  }

  //todo:add invalidate by id
  def invalidate(instance : UUID, systemInvalidationPath : String){
    val client = getConnection(instance)
    if (client.checkExists.forPath(systemInvalidationPath+"/doit")==null)
      client.create().forPath(systemInvalidationPath+"/doit")
    else
      client.delete().forPath(systemInvalidationPath+"/doit")
  }



  def doesExist( instance :UUID,  key : String) : Boolean = {
    val client = getConnection(instance)
    val (basePath,_,_)= registration(instance)
    if (client.checkExists().forPath(basePath+key)!=null) true else false
  }



  def put(instance : UUID,key :String, input : Array[Byte], ttl: Long = ZooCache.FOREVER):Boolean ={
    val client = getConnection(instance)
    val (basePath,_,useLocalShadow)= registration(instance)

    def putBytes (input :Array[Byte],ttl: Array[Byte]):Boolean  ={
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


    val meta=new ItemMetadata()
    meta.ttl= ttl
    val metaBytes=pack(meta)
    val wasSuccessful=putBytes(input,metaBytes)

    if (wasSuccessful && useLocalShadow) {
      putLocalCopy(key, input, meta)
    }

    wasSuccessful
  }


  private def putLocalCopy(key: String, input: Array[Byte], meta: ItemMetadata) {
    shadowActor ! UpdateLocal(key,input)
    shadowActor ! UpdateLocal(key + ZooCache.TTL_PATH, meta)
  }

  def get(instance : UUID, key: String) : Option[Array[Byte]]= {

    val client = getConnection(instance)
    val (basePath,_,useLocalShadow)= registration(instance)
     def  getBytes(path : String) :Option[Array[Byte]] ={
      val fullPath=basePath+path
      try {
        if (client.checkExists().forPath(fullPath) == null) None
        else
          Some(client.getData.forPath(fullPath))
      } catch {
        case e: Exception => {
          error("can't update '"+ key+"' in Zookeeper",e)
          None
        }
      }

    }

    def isInShadow:Boolean ={
      if (!useLocalShadow) return false
      val reply=Await.result(shadowActor ? GetLocal(key+ZooCache.TTL_PATH), 1 second).asInstanceOf[Option[ItemMetadata]]
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


    if (isInShadow) return  Some(Await.result(shadowActor ? GetLocal(key), 1 second).asInstanceOf[Array[Byte]])

    isInCache match{
      case None => None
      case Some(meta) => {

        val result=getBytes(key)
        if (useLocalShadow) putLocalCopy(key,result.get,meta)
        result
      }
    }
  }




  def removeItem(instance : UUID, key: String) {
    val client = getConnection(instance)
    val (basePath,_,useLocalShadow)= registration(instance)
    remove(basePath+key)

    def remove(path :String){
      val children=client.getChildren.forPath(path)

      for (child <- children) {
        remove(path+"/"+child)
      }

      client.inTransaction().
         delete().forPath(path).
        and().
         delete().forPath(path+ZooCache.TTL_PATH).
        and().
        commit()

      if(useLocalShadow) {
         shadowActor ! RemoveLocal(key)
         shadowActor ! RemoveLocal(key+ZooCache.TTL_PATH)
      }
    }

  }


}
