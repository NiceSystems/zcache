package com.nice.zoocache

import akka.actor.Actor
import grizzled.slf4j.Logging
import com.netflix.curator.framework.CuratorFramework
import collection.JavaConversions._
import org.msgpack.ScalaMessagePack._
import com.netflix.curator.framework.recipes.leader.{LeaderSelector, LeaderSelectorListener}
import com.netflix.curator.framework.state.ConnectionState
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

case class Tick(client : CuratorFramework)


class Scavenger extends Actor with Logging{

  protected def receive = {
    case Tick(client) => {
      val listener = new LeaderSelectorListener()
      {
        @Override
        def takeLeadership( client :CuratorFramework)  {
          clean(client)
        }

        @Override
        def stateChanged( client :CuratorFramework,  newState :ConnectionState)
        {
        }
      }
      val selector = new LeaderSelector(client, "/leader", listener)
      selector.start()
    }
  }


  private[zoocache] def clean(client : CuratorFramework){

    cleaner(ZooCache.CACHE_ID)

    def cleaner(path : String){
      val children=client.getChildren.forPath(path)
      for (child <- children) {
        if (child!=ZooCache.TTL_PATH) {  //assumption only leaf nodes have ttl!
          cleaner(path :> child) }
        else validateTtl(path)
        }
     }

     def validateTtl(itemPath :String) {
      val ttlPath=itemPath :> ZooCache.TTL_PATH
      if (client.checkExists().forPath(ttlPath)== null) return

      try{
        val meta=unpack[ItemMetadata](client.getData.forPath(ttlPath))
        if (!meta.isValid) {
          client.inTransaction().
            delete().forPath(ttlPath).
            and().
            delete().forPath(itemPath).
            and().
            commit()}
      }
      catch {
        case e : Exception => {
          error(e)
          false
        }
      }
    }
    }


}
