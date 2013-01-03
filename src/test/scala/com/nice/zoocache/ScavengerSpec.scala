package com.nice.zoocache

import org.scalatest.{BeforeAndAfterAll, FunSpec}
import com.netflix.curator.test.TestingServer
import akka.testkit.{TestKit, TestActorRef, TestActor}
import com.netflix.curator.framework.CuratorFrameworkFactory
import com.netflix.curator.retry.ExponentialBackoffRetry
import akka.actor.ActorSystem
import akka.util.duration._


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
  val cache=new ZooCache(testCluster,"test")
  val client = CuratorFrameworkFactory.builder().
    connectString(testCluster).
    namespace("/cache").
    retryPolicy(retryPolicy).
    build
   client.start()
  implicit val  testSystem= ActorSystem();
  var testScavenger=TestActorRef(new Scavenger(client)).underlyingActor


  it("can remove all items"){
    val t=new Test()
    t.name="tada"
    for (i<-1 to 10)
      cache.put(i.toString,t,1)

    Thread.sleep(10)

    testScavenger.clean

    println(client.getChildren().forPath("/test").size())
    assert(client.getChildren().forPath("/test").isEmpty)
  }

  it("doesn't remove items that aren't expired") {
    val t=new Test()
    t.name="tada"
    cache.put("1",t,ZooCache.FOREVER)
    for (i<-2 to 11)
      cache.put(i.toString,t,1)

    Thread.sleep(10)

    testScavenger.clean

    val children=client.getChildren().forPath("/test")
    assert(children.size()==1)
    assert(children.get(0)=="1")
  }

  it("runs automatically in a ZooCache instance") {
    val path="independent"

    val newCache=new ZooCache(testCluster,path,interval = 50 milliseconds)

    addToCache(newCache,path)
    newCache.shutdown()
  }


  def addToCache(newCache: ZooCache, path: String) {
    val t = new Test()
    t.name = "tada"
    newCache.put("1", t, ZooCache.FOREVER)
    for (i <- 2 to 11)
      newCache.put(i.toString, t, 1)

    Thread.sleep(100)

    val children = client.getChildren().forPath("/"+path)
    assert(children.size() == 1)
    assert(children.get(0) == "1")
  }

  it("runs scavenger periodically") {
    val path="another"
    val newCache=new ZooCache(testCluster,path,interval = 50 milliseconds)
    addToCache(newCache,path)
    addToCache(newCache,path)
    newCache.shutdown()

  }


  it("deosn't interfere with other caches") (pending)
  it("can handle items that have a parent") (pending)

  it("can coordinate with multiple Scavenger instances") (pending)

  override def afterAll{
    testSystem.shutdown()
    client.close()
    cache.shutdown()
  }

}
