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
import org.scalatest.{BeforeAndAfterAll, FunSpec}
import com.netflix.curator.test.{TestingCluster, TestingServer}
import org.msgpack.ScalaMessagePack._
import org.msgpack.ScalaMessagePack
import scala.Some
import java.lang.{String => JString}
import scala.Predef.String
import akka.util.duration._
import akka.pattern.ask


import akka.actor.ActorSystem
import akka.testkit.TestActorRef
import akka.dispatch.Await
import java.util.UUID
import akka.util.Timeout
import com.nice.zoocache

class ZooCacheActorSpec extends FunSpec with BeforeAndAfterAll {

  var server=new TestingServer()
  var testCluster=server.getConnectString
  //var testCluster="hadoop2"
  implicit val  testSystem= ActorSystem();
  var testCache=TestActorRef(new ZooCacheActor)
  var id : UUID = null
  implicit val timeout = Timeout(1 hour)


  override def beforeAll()={
     id = Await.result(testCache ? Register("test",testCluster,false), 1 hour).asInstanceOf[UUID]
  }


  it("should do a simple put/get Bytes"){
    val t=new Test()
    t.name="Arnon"

    testCache ! Put(id,"test",ScalaMessagePack.write(t),ZooCache.FOREVER)
    val value = Await.result(testCache ? GetValue(id,"test"), 1 hour).asInstanceOf[Option[Array[Byte]]]
    value match {
      case Some(result) => assert(unpack[Test](result).name===t.name)
      case None => assert(false)
    }
  }

  it("can remove items from cache") {
      val t=new Test()
      t.name="Arnon"

      testCache ! Put(id,"test",ScalaMessagePack.write(t),ZooCache.FOREVER)
    val value = Await.result(testCache ? GetValue(id,"test"), 1 hour).asInstanceOf[Option[Array[Byte]]]
    value match {
      case Some(result) => assert(unpack[Test](result).name===t.name)
      case None => assert(false)
    }
      testCache ! RemoveKey(id,"test")
      val value2 = Await.result(testCache ? GetValue(id,"test"), 1 hour).asInstanceOf[Option[Array[Byte]]]
      value2 match {
        case Some(result) => assert(false)
        case None => assert(true)
      }

  }

  it("can check exsitance") {
    val t=new Test()
    t.name="Arnon"
    testCache ! Put(id,"test",ScalaMessagePack.write(t),ZooCache.FOREVER)
    val value1 = Await.result(testCache ? Exists(id,"test"), 1 second).asInstanceOf[Boolean]
    assert(value1)
    val value2 = Await.result(testCache ? Exists(id,"blah"), 1 second).asInstanceOf[Boolean]
    assert(!value2)
  }

  it("can serve more than one connection") {
    val server2=new TestingServer()
    val testCluster2=server2.getConnectString
    val id2 = Await.result(testCache ? Register("test",testCluster2,false), 1 hour).asInstanceOf[UUID]

    val t=new Test()
    t.name="Arnon"
    testCache ! Put(id,"test-1",ScalaMessagePack.write(t),ZooCache.FOREVER)
    testCache ! Put(id2,"test-2",ScalaMessagePack.write(t),ZooCache.FOREVER)
    assert(Await.result(testCache ? Exists(id,"test-1"), 1 second).asInstanceOf[Boolean])
    assert(!Await.result(testCache ? Exists(id2,"test-1"), 1 second).asInstanceOf[Boolean])
    assert(Await.result(testCache ? Exists(id2,"test-2"), 1 second).asInstanceOf[Boolean])
    assert(!Await.result(testCache ? Exists(id,"test-2"), 1 second).asInstanceOf[Boolean])

    server2.close()
  }

  it("doesn't fail on invalid connection"){
    val id2 = Await.result(testCache ? Register("test","blah-cluster",false), 1 hour).asInstanceOf[UUID]

    val t=new Test()
    t.name="Arnon"
    testCache ! Put(id2,"test-2",ScalaMessagePack.write(t),ZooCache.FOREVER)
  }


  it("doesn't fail on failed server") (pending)
  override def afterAll{
     testCache ! Shutdown
    server.close()
  }


}
