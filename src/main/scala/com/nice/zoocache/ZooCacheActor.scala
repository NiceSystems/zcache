package com.nice.zoocache

import akka.util.{Timeout, Duration}
import grizzled.slf4j.Logging
import com.netflix.curator.retry.ExponentialBackoffRetry
import com.netflix.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import akka.actor.{ActorRef, Actor}
import org.apache.zookeeper.{WatchedEvent, Watcher}
import org.msgpack.ScalaMessagePack._

import scala.Some
import akka.dispatch.Await
import akka.util.duration._
import akka.pattern.ask
import java.util.UUID
import collection.JavaConversions._
import PathString._


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
case class Register(basePath : String, zookeeperConnection : String,useLocalShadow : Boolean, interval : Duration = 30 minutes)

class ZooCacheActor extends Actor with Logging {




  implicit val timeout = Timeout(1 second)

  val shadowActor:ActorRef = context.actorFor("../"+ZooCache.LOCALSHADOW)
  val scavenger = context.actorFor("../"+ZooCache.SCAVENGER)


  var connections = Map[String,Option[CuratorFramework]]()
  var registration = Map[UUID,(String, String, Boolean,Duration)]()
  var watchers = List[Watcher]()


    def receive ={
      case Put(instance,key,value,ttl) => sender ! put(instance,key,value,ttl)
      case Invalidate(instance,path) => invalidate(instance,path)
      case GetValue(instance,key)=> sender ! get(instance,key)
      case RemoveKey(instance,key)=> removeItem(instance,key)
      case Exists(instance,key) => sender ! doesExist(instance,key)
      case Shutdown() =>  connections.values.foreach(_.get.close())
      case Register(basePath,connection,useLocalShadow,interval) => sender ! register(basePath,connection,useLocalShadow,interval)

    }

  private def register(basePath : String, zookeeperConnection : String,useLocalShadow : Boolean, interval : Duration) : UUID ={


    val id =UUID.randomUUID()
    val path=ZooCache.CACHE_ROOT :/ basePath
    val invalidationPath= ZooCache.INVALIDATE_PATH :> basePath

    registration = registration+ (id -> (path,zookeeperConnection,useLocalShadow,interval))

    if(useLocalShadow) setupInvalidation()
    def setupInvalidation(){
      getConnection(id) match {
        case Some(client) => {
          ensurePath(client,invalidationPath)
          setupWatcher(client)
        }
      }
    }


    def setupWatcher(client : CuratorFramework)  {
      lazy val watcher : Watcher= new Watcher() {
        override def process(event: WatchedEvent) {
          try {
            //reset the watch as they are one-time
            client.getChildren.usingWatcher(watcher).forPath(invalidationPath)
            //shadow.clear()
            shadowActor ! ClearMemory() // change to ClearMemory(id)
          } catch {
            case e: InterruptedException =>  error("problem processing invalidation event",e)
          }
        }
      }
      client.getChildren.usingWatcher(watcher).forPath(invalidationPath)
      watchers = watcher :: watchers
    }

    id

  }

  private def getConnection(instance : UUID): Option[CuratorFramework] = {

    val (_,connectionString,_,interval) = registration(instance)

    def establishConnection = {
       val retryPolicy = new ExponentialBackoffRetry(100,3)
        val newConn = CuratorFrameworkFactory.builder().
          connectString(connectionString).
          retryPolicy(retryPolicy).connectionTimeoutMs(1000).
          build
        try {
        newConn.start()
        newConn.checkExists.forPath(ZooCache.CACHE_ROOT)

        connections= connections + (connectionString->Some(newConn))
        val sched=ZooCache.system.scheduler.schedule(0 seconds,interval, scavenger, Tick(newConn))


        Some(newConn)
        } catch {
          case e:Exception => {
            error(e)
            None
          }
        }
      }

    connections.getOrElse(connectionString,establishConnection)
  }


  private def ensurePath(cl:CuratorFramework, path:String) {
    val ensurePath = cl.newNamespaceAwareEnsurePath(path)
    ensurePath.ensure(cl.getZookeeperClient)
  }

  //todo:add invalidate by id
  def invalidate(instance : UUID, systemInvalidationPath : String){
    getConnection(instance) match {
      case Some(client) => {
          if (client.checkExists.forPath(systemInvalidationPath :> "doit")==null)
            client.create().forPath(systemInvalidationPath :> "doit")
          else
            client.delete().forPath(systemInvalidationPath :> "doit")
          }
    }
  }



  def doesExist( instance :UUID,  key : String) : Boolean = {
    val (basePath,_,_,_)= registration(instance)
    getConnection(instance) match {
      case Some(client) => if (client.checkExists().forPath(basePath :> key)!=null) true else false
      case None => false
    }
  }



  def put(instance : UUID,key :String, input : Array[Byte], ttl: Long = ZooCache.FOREVER):Boolean ={
    val hasClient=getConnection(instance)
    if (hasClient.isEmpty) return false

    val client =  hasClient.get

    val (basePath,_,useLocalShadow,_)= registration(instance)

    def putBytes (input :Array[Byte],ttl: Array[Byte]):Boolean  ={
      val path=basePath :> key
      val ttlPath=path :> ZooCache.TTL_PATH

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
      putLocalCopy(instance,key, input, meta)
    }

    wasSuccessful
  }


  private def putLocalCopy(id: UUID,key: String, input: Array[Byte], meta: ItemMetadata) {
    shadowActor ! UpdateLocal(id.toString :> key,input)
    shadowActor ! UpdateLocal(id.toString :> key :> ZooCache.TTL_PATH, meta)
  }

  def get(instance : UUID, key: String) : Option[Array[Byte]]= {
    val hasClient=getConnection(instance)
    if (hasClient.isEmpty) return None
    val client =  hasClient.get

    val (basePath,_,useLocalShadow,_)= registration(instance)
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
      val reply=Await.result(shadowActor ? GetLocal(instance.toString :> key :> ZooCache.TTL_PATH), 1 second).asInstanceOf[Option[ItemMetadata]]
      reply match
      {
        case Some(metadata)=>  metadata.isValid
        case None =>  false
      }
    }

    def isInCache:Option[ItemMetadata] ={
      getBytes(key :> ZooCache.TTL_PATH) match {
        case Some(meta) => {
          val result=unpack[ItemMetadata](meta)
          if (result.isValid) Some(result) else None
        }
        case None =>None
      }
    }


    if (isInShadow) return  Await.result(shadowActor ? GetLocal(instance.toString :>  key), 1 second).asInstanceOf[Option[Array[Byte]]]

    isInCache match{
      case None => None
      case Some(meta) => {

        val result=getBytes(key)
        if (useLocalShadow) putLocalCopy(instance,key,result.get,meta)
        result
      }
    }
  }




  def removeItem(instance : UUID, key: String) {
    val hasClient=getConnection(instance)
    if (hasClient.isEmpty) return

    val client =  hasClient.get

    val (basePath,_,useLocalShadow,_)= registration(instance)
    if (doesExist(instance,key))  remove(basePath :> key)

    def deleteNode(path: String) {
      if (client.checkExists().forPath(path :> ZooCache.TTL_PATH)!=null) {
        client.inTransaction().
          delete().forPath(path :> ZooCache.TTL_PATH).
          and().
          delete().forPath(path).
          and().
          commit()

        if(useLocalShadow) shadowActor ! RemoveLocal(instance.toString :> key :> ZooCache.TTL_PATH)
      } else {
        client.delete().forPath(path)
      }

      if (useLocalShadow) {
        shadowActor ! RemoveLocal(instance.toString :> key)

      }
    }

    def remove(path :String){
      val children=client.getChildren.forPath(path)
      for (child <- children) {
        if ("/"+child!=ZooCache.TTL_PATH) {  //assumption only leaf nodes have ttl!
              remove(path :> child)
        }
      }
      deleteNode(path)
    }

  }


}
