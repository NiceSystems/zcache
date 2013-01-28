package com.nice.zoocache

import akka.actor.Actor
import grizzled.slf4j.Logging
import com.netflix.curator.framework.CuratorFramework
import collection.JavaConversions._
import org.msgpack.ScalaMessagePack._
import com.netflix.curator.framework.recipes.leader.{LeaderSelector, LeaderSelectorListener}
import com.netflix.curator.framework.state.ConnectionState
import PathString._
import akka.util.Duration
import java.util.Date
import sun.jvm.hotspot.runtime.Bytes


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

private[zoocache] case class Tick(client : CuratorFramework, interval: Duration)


private[zoocache] class Scavenger extends Actor with Logging{

  protected def receive = {
    case Tick(client,interval) =>  tryCleanup(client,interval)
  }


  def tryCleanup( client: CuratorFramework,interval: Duration) {
    val listener = new LeaderSelectorListener() {
      @Override
      def takeLeadership(client: CuratorFramework) {
        if (shouldIRun(client, interval)) {
          clean(client)
          markLastRun(client)
        }
      }

      @Override
      def stateChanged(client: CuratorFramework, newState: ConnectionState) {
      }
    }
    val selector = new LeaderSelector(client, ZooCacheSystem.LEADER_PATH, listener)
    selector.start()
  }

  // relies on synchronized clocks (not a prob if off by a few
  // but can be problematic if off by a few hours)
  def shouldIRun(client : CuratorFramework,interval :Duration) : Boolean={
    ensurePath(client)
     val lastRunBytes=client.getData.forPath(ZooCacheSystem.LAST_RUN_PATH)
     if (lastRunBytes.isEmpty) return true // probably first run, worst case we get an additional cleanup
     val lastRun=unpack[Long](lastRunBytes)
     val now = new Date().getTime

     now-lastRun>interval.length
  }


  def ensurePath(client: CuratorFramework) {
    val ensurePath = client.newNamespaceAwareEnsurePath(ZooCacheSystem.LAST_RUN_PATH)
    ensurePath.ensure(client.getZookeeperClient)
  }

  def markLastRun(client : CuratorFramework){
      ensurePath(client)
      val now=pack(new  Date().getTime)
      client.setData().forPath(ZooCacheSystem.LAST_RUN_PATH,now)
  }
  def clean(client : CuratorFramework){
    if(client.checkExists.forPath(ZooCacheSystem.CACHE_ID)==null) return // no cache to clean

    cleaner(ZooCacheSystem.CACHE_ID)

    def cleaner(path : String){
      val children=client.getChildren.forPath(path)
      for (child <- children) {
        if (child!=ZooCacheSystem.TTL_PATH) {  //assumption only leaf nodes have ttl!
          cleaner(path :> child) }
        else validateTtl(path)
        }
     }

     def validateTtl(itemPath :String) {
      val ttlPath=itemPath :> ZooCacheSystem.TTL_PATH
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
