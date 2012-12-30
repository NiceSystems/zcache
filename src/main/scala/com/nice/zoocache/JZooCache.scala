package com.nice.zoocache
/**
 * User: arnonrgo
 * Date: 12/30/12
 * Time: 2:25 PM
 */

// Java friendly API
class JZooCache(connectionString: String,systemId : String){
     val cache= new ZooCache(connectionString,systemId,useLocalShadow=true)

  def put(key: String, value: Any) = {
    cache.put(key,value,ZooCache.FOREVER)
  }


  def get[T](key: String, jType: Class[T]) : T ={
     cache.get(key).get
  }
}
