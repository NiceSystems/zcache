package com.nice.zoocache

import org.scalatest.{BeforeAndAfterAll, FunSpec}
import com.netflix.curator.test.TestingServer
import akka.testkit.{TestKit, TestActorRef, TestActor}
import com.netflix.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import com.netflix.curator.retry.ExponentialBackoffRetry
import akka.actor.ActorSystem
import akka.util.duration._
import PathString._


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
class ScavengerSpec extends FunSpec with BeforeAndAfterAll {

  val server=new TestingServer()
  val testCluster=server.getConnectString
  private val retryPolicy = new ExponentialBackoffRetry(1000, 10)
  val systemId="scavange"
  val cache=new ZooCache(testCluster,systemId,maxWait = 10 seconds)
  val client = CuratorFrameworkFactory.builder().
    connectString(testCluster).
    retryPolicy(retryPolicy).
    build
   client.start()
  implicit val  testSystem= ActorSystem()
  var testScavenger=TestActorRef(new Scavenger).underlyingActor
  Thread.sleep(100)


  it("can remove all items"){
    val t=new Test()
    t.name="tada"
    for (i<-1 to 10)
      cache.put(i.toString,t,1)


   val y=client.getChildren.forPath(ZooCache.CACHE_ID :> systemId)
    println(y.size())
    Thread.sleep(10)

    testScavenger.clean(client)

    println(client.getChildren.forPath(ZooCache.CACHE_ID :>  systemId).size())
    assert(client.getChildren.forPath(ZooCache.CACHE_ID :> systemId).isEmpty)
  }

  it("doesn't remove items that aren't expired") {
    val t=new Test()
    t.name="tada"
    cache.put("1",t,ZooCache.FOREVER)
    for (i<-2 to 11)
      cache.put(i.toString,t,1)

    Thread.sleep(10)

    testScavenger.clean(client)

    val children=client.getChildren.forPath(ZooCache.CACHE_ID :> systemId)
    assert(children.size()==1)
    assert(children.get(0)=="1")
  }

  it("runs automatically in a ZooCache instance") {
    val path="independent"
    val independentServer=new TestingServer(9998)
    val newCache=new ZooCache(independentServer.getConnectString,path,interval = 50 milliseconds,maxWait = 1 hour)
    val indie = CuratorFrameworkFactory.builder().
      connectString(independentServer.getConnectString).
      retryPolicy(retryPolicy).
      build
    indie.start()

    addToCache(newCache,path)
    checkCache(path,1,indie)

    independentServer.close()
  }


  def addToCache(newCache: ZooCache, path: String) {
    val t = new Test()
    t.name = "tada"
    newCache.put("1", t, ZooCache.FOREVER)
    for (i <- 2 to 11)
      newCache.put(i.toString, t, 1)

    Thread.sleep(200)

  }


  def checkCache(path: String,size :Int=1, cl : CuratorFramework = client) {
    val children = cl.getChildren.forPath(ZooCache.CACHE_ID :> path)
    assert(children.size() == size)
    println(size)
    assert(children.get(0) == "1")
  }

  it("runs scavenger periodically") {
    val path="repeater"
    val repeatServer=new TestingServer(9999)
    val newCache=new ZooCache(repeatServer.getConnectString,path,interval = 50 milliseconds,maxWait = 1 hour)
    addToCache(newCache,path)
    val repeatClient = CuratorFrameworkFactory.builder().
      connectString(repeatServer.getConnectString).
      retryPolicy(retryPolicy).
      build
    repeatClient.start()
    checkCache(path,cl=repeatClient)

    addToCache(newCache,path)
    Thread.sleep(100)
    checkCache(path,cl=repeatClient)

    repeatServer.close()
    repeatClient.close()
  //  newCache.shutdown()

  }


  it("one cache can clean all"){

    val oneCache=new TestingServer(10000)
    val oneClient = CuratorFrameworkFactory.builder().
      connectString(oneCache.getConnectString).
      retryPolicy(retryPolicy).
      build
    oneClient.start()
    val fastPath="fast"
    val fast=new ZooCache(oneCache.getConnectString,fastPath,interval = 50 milliseconds,maxWait =  2 seconds)
    val slowPath="slow"
    val slow=new ZooCache(oneCache.getConnectString,slowPath,interval = 50 milliseconds,maxWait = 2 seconds)
    Thread.sleep(100)

    addToCache(slow,slowPath)
    addToCache(fast,fastPath)
    checkCache(fastPath,cl=oneClient)
    checkCache(slowPath,cl=oneClient)
    oneCache.close()
    oneClient.close()
  }



  it("can coordinate with multiple Scavenger instances") {
    val fastPath="fast"
    val fast=new ZooCache(testCluster,fastPath,interval = 50 milliseconds)
    val slowPath="slow"
    val slow=new ZooCache(testCluster,slowPath,interval = 60 milli)
    Thread.sleep(1000)
  }



  it("can handle items that have a parent") (pending)
  it("scavenges tied to different zookeeper instances work in parallel") (pending)

  override def afterAll(){
    testSystem.shutdown()
    client.close()
   // cache.shutdown()
  }

}
