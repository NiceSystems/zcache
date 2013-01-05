package com.nice.zoocache

import akka.actor.Actor
import grizzled.slf4j.{Logging}
import com.netflix.curator.framework.CuratorFramework
import collection.JavaConversions._
import org.msgpack.ScalaMessagePack._


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

case class Tick()

class Scavenger(client : CuratorFramework) extends Actor with Logging{

  protected def receive = {
    case Tick => clean
  }

  // form a cluster with other Zoocache clients
  // leader elect an active scavanger



  private[zoocache] def clean{

    cleaner(ZooCache.CACHE_ROOT)


    def cleaner(path:String) {
    try{
      for (childPath<-client.getChildren().forPath(path)){
        val childFullPath=path+"/"+childPath
        if ( checkTtl(childFullPath)) {
          removeItem(childFullPath)
        }
        cleaner(childFullPath)
      }
    }  catch {
        case e : Exception => error(e)
      }
    // get children
    // foreach check Metadata and TTL
    // if expired - remove item
    }
  }

  private def checkTtl(basePath :String):Boolean = {
    val path=basePath+ZooCache.TTL_PATH
   if (client.checkExists().forPath(path)== null) return false

    try{
      val meta=unpack[ItemMetadata](client.getData().forPath(path))
      !meta.isValid
    }
    catch {
      case e : Exception => {
           error(e)
            false
      }
    }
  }

  private def removeItem(key: String) {
     val path=if (key.startsWith("/")) key else  "/"+key
     val children=client.getChildren.forPath(path)

     for (child <- children) {
       removeItem(key+"/"+child)
     }
     client.delete().forPath(path)
  }

}
