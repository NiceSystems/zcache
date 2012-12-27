package com.nice.zoocache

import org.scalatest.FunSpec
import com.netflix.curator.test.TestingServer

/**
 * User: arnonrgo
 * Date: 12/27/12
 * Time: 2:15 PM
 */
class LocalShadowSpec extends FunSpec {
  val server=new TestingServer()
  val testCluster=server.getConnectString
  //var testCluster="10.211.55.25:2181"
  val cache  = new ZooCache(testCluster,"test",useLocalShadow = true)
  val second = new ZooCache(testCluster,"test",useLocalShadow = true)



  it("should fetch from remote cache if local copy is mising"){
    val t=new Test()
    t.name="remote"
    val key="remote"

    second.put(key,t)
    val result=cache.get[Test](key)
    assert(result.get.name===t.name)

  }

  it("should serve local copy if local copy is not expired") {
    val t=new Test()
    t.name="remote"
    val key="lchanged"

    second.put(key,t,ZooCache.FOREVER)
    val firstResult=cache.get[Test](key)

    t.name="lchanged"
    second.put(key,t,ZooCache.FOREVER)
    val secondResult=cache.get[Test](key)

    assert(firstResult.get.name===secondResult.get.name)
  }

  it("should fetch from remote cache if local copy expired") {
    val t=new Test()
    t.name="remote"
    val key="changed"

    second.put(key,t,1)
    val firstResult=cache.get[Test](key)
    assert(firstResult.get.name===t.name)

    Thread.sleep(2000)
    t.name="changed"
    second.put(key,t,ZooCache.FOREVER)
    val secondResult=cache.get[Test](key)
    assert(secondResult.get.name===t.name)

  }

  it("should invalidate local copy if asked") (pending)
}
