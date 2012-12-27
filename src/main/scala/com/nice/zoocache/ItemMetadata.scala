package com.nice.zoocache

import org.msgpack.annotation.Message
import java.util.Date
import org.msgpack.{ScalaMessagePack, MessagePack}
import java.util.Date


/**
 * User: arnonrgo
 * Date: 12/26/12
 * Time: 5:57 PM
 */
@Message
class ItemMetadata(){
  var updateTime  = new Date().getTime
  def expirationTime = if (ttl!=ZooCache.FOREVER) updateTime + ttl else  Long.MaxValue
  var ttl : Long= ZooCache.FOREVER

}
