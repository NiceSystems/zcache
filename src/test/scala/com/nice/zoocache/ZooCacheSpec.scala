package com.nice.zoocache

import org.scalatest.{BeforeAndAfterAll, FunSpec}
import com.netflix.curator.test.TestingServer
import org.msgpack.ScalaMessagePack._
import org.msgpack.ScalaMessagePack
import scala.Some

/**
 * User: arnonrgo
 * Date: 12/26/12
 * Time: 12:01 PM
 */
class ZooCacheSpec extends FunSpec with BeforeAndAfterAll {

  var server=new TestingServer()
  var testCluster=server.getConnectString
  //var testCluster="10.211.55.25:2181"
  var cache=new ZooCache(testCluster,"test")

  it("should connect to the cluster"){
    val tempCache=new ZooCache(testCluster,"test")
  }

  it("should do a simple put/get Bytes"){
    val t=new Test()
    t.name="Arnon"

    cache.putBytes("test",ScalaMessagePack.write(t))
    val value = cache.getBytes("test")
    value match {
      case Some(result) => assert(unpack[Test](result).name===t.name)
      case None => assert(false)
    }


  }

   it("should be able to write twice to same key (last wins)"){
    val t=new Test()
    t.name="Arnon"
    val key="myValue"

    cache.put(key,t)

    t.name="Not Arnon"
    cache.put(key,t)


    val result=cache.get[Test](key).get
    assert(result.name!="Arnon")

  }

  it("should put an object and retrieve it"){
    val t=new Test()
    val key="myValue9"

    t.name="MyName"

    cache.put(key,t)

    val value=cache.getBytes(key).get
    assert(unpack[Test](value).name===t.name)
  }

  it("should get object back by generics"){
    val t=new Test()
    val key="myValue2"

    t.name="MyName2"

    cache.put(key,t)

    val value = cache.get[Test](key).get
    assert(value.name===t.name)
  }

  it("can put/get a 2 key hierarchy "){
    val t1=new Test()
    t1.name="first"

    val t2=new Test()
    t2.name="second"
    val parent="parent"
    val key1="child1"
    val key2="child2"

    cache.put(parent,key1,t1)
    cache.put(parent,key2,t2)

    assert(cache.get[Test](parent,key2).get.name===t2.name)
    assert(cache.get[Test](parent,key1).get.name===t1.name)
  }

   it("can verify an item is in the cache"){
     val t1=new Test()
     t1.name="first"
     val key ="k"
     cache.put(key,t1)

     assert(cache.doesExist(key))
     assert(!cache.doesExist("blah"))

   }

  it("can get remove all for a parent key"){
    val t1=new Test()
    t1.name="first"

    val t2=new Test()
    t2.name="second"
    val parent="newparent"
    val key1="child1"
    val key2="child2"

    cache.put(parent,key1,t1)
    cache.put(parent,key2,t2)

    val results=cache.removeAll(parent)
    assert(!cache.doesExist(parent+"/"+key1))
  }

  it("getBytes should retun null on invalid keys"){
    assert(cache.getBytes("blah")==None)
  }

  it("should retun null on invalid keys"){
    assert(cache.get[Test]("blah")==None)
  }

  it("should serve diffrent caches to different apps"){

    val otherCache= new ZooCache(testCluster,"otherApp")
    otherCache.put("1",new Test())

    assert(cache.get[Test]("1")==None)
  }


  it("should serve same cache to different instances"){

    val otherCache= new ZooCache(testCluster,"test")
    val t=new Test()
    t.name="same"
    otherCache.put("1",t)

    assert(cache.get[Test]("1").get.name==="same")
  }

  it("should do a simple put/get  with memory shadow"){
    val t=new Test()
    t.name="Arnon"
    val shadowCache= new ZooCache(testCluster,"test",useLocalShadow = true)

    shadowCache.put("test",t)
    val value = shadowCache.get[Test]("test")
    value match {
      case Some(result) => assert(result.name===t.name)
      case None => assert(false)
    }


  }

  it("should expire value if TTL passed"){
    val t1=new Test()
    t1.name="old"
    val key="expired"
    cache.put(key,t1,5)

    Thread.sleep(500)
    val value=cache.get[Test](key)
    assert(value==None)
  }



}
