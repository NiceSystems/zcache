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

  def isValid:Boolean ={
    val current=new Date().getTime
    current<expirationTime
  }
}
