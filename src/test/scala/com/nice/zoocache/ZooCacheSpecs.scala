package com.nice.zoocache

import com.netflix.curator.test.TestingServer
import akka.util.duration._
import org.scalatest.{BeforeAndAfterAll, FunSpec}
import akka.dispatch.Await


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
class ZooCacheSpecs extends FunSpec with BeforeAndAfterAll {



  val maxWait=2 seconds

  it("can put values in case"){
    val server=new TestingServer()
    val testCluster=server.getConnectString
    //var testCluster="hadoop2"
    val cache=new ZooCache(testCluster,"test",maxWait=10 seconds)
    val t=new Test()

    val key="myValue2"

    t.name="MyName2"

    val retValue =cache.put(key,t)

   /* Thread.sleep(1000)
    retValue onComplete {
      case Right(result : Boolean) => expect(true) {result}
      case Left(failure) => assert(false)
      case _ => assert(false)
    }
    */

    expect(true) (Await.result(retValue,maxWait).asInstanceOf[Boolean])
    server.close()
  }
  it("can get values back"){
    val server=new TestingServer()
    val testCluster=server.getConnectString
    //var testCluster="hadoop2"
    val cache=new ZooCache(testCluster,"test",maxWait=10 seconds)
    val t=new Test()

    val key="myValue2"

    t.name="MyName2"

    cache.put(key,t)
    val retValue=cache.get[Test](key)

    /* Thread.sleep(1000)
     retValue onComplete {
       case Right(result : Boolean) => expect(true) {result}
       case Left(failure) => assert(false)
       case _ => assert(false)
     }
     */

    expect(t.name) (Await.result(retValue,maxWait).get.name)
    server.close()
  }
}
