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

import com.netflix.curator.retry.ExponentialBackoffRetry

import org.msgpack.ScalaMessagePack._
import collection.JavaConversions._
import org.apache.zookeeper.{WatchedEvent, Watcher}
import com.netflix.curator.framework
import framework.{CuratorFramework, CuratorFrameworkFactory}
import grizzled.slf4j.Logging
import akka.actor.{Props, ActorSystem}
import akka.pattern.ask
import akka.dispatch.Await
import akka.util.duration._
import akka.util.{Duration, Timeout}
import java.util.UUID
import PathString._

/**
 * User: arnonrgo
 * Date: 12/26/12
 * Time: 10:47 AM
 */


object ZooCacheSystem  {

  val FOREVER : Long= -2
  private val MASTER_PATH : PathString = "zooCache"
  private[zoocache] val TTL_PATH = "ttl"
  private[zoocache] val CACHE_ID : PathString= MASTER_PATH :> "cache"
  private[zoocache] val LAST_RUN_PATH : PathString =MASTER_PATH :> "lastRun"
  private[zoocache] val LEADER_PATH : PathString=MASTER_PATH :> "leader"
  private[zoocache] val INVALIDATE_PATH=CACHE_ID :> "invalidate"
  private val DEFAULT_LOCAL_CACHE_SIZE = 10000

  //Actor names
  private[zoocache] val LOCALSHADOW =  "LocalShadow"
  private[zoocache] val SCAVENGER = "Scavenger"

  private[zoocache] val system=ActorSystem(CACHE_ID.noPath)
  private val scavenger=system.actorOf(Props(new Scavenger),name=SCAVENGER)
  private val shadowActor=system.actorOf(Props(new LocalShadow(DEFAULT_LOCAL_CACHE_SIZE)), name =LOCALSHADOW)
  private val cache=system.actorOf(Props(new ZooCacheActor()), name =CACHE_ID.noPath)

  def getCacheActor = system.actorFor(system / CACHE_ID.noPath)
}


