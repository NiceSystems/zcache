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
trait ZCache {

    /**
     * Add a value to the cache with a time-to-live (ttl)
     *
     * @param key the id (string) of for the value
     * @param input the object to put in the cache
     * @param ttl time to live in miliseconds
     */
    def put(key :String, input : Any, ttl: Long = ZooCache.FOREVER):Boolean

  /**
     * Add a value to the cache with a time-to-live (ttl) under a parent group
     *
     * @param parent the id (string) of the group
     * @param key the id (string) of for the value
     * @param value the object to put in the cache
     * @param ttl time to live in miliseconds
     */
    def put(parent: String, key : String, value: Any,ttl : Long): Boolean

    /**
     * Remove all the items from a group
     *
     * @param key the id (string) of the group  or item to remove
     */

    def removeItem(key: String)

    /**
     * invalidate the local cache on all clients - can be used by any client of the cache
     * for instance you cna add configurations to the cache with a TTL forever so clients will use local copies
     * but invalidate all the clients if the configuration changes anyway
     */
    def invalidate()

    /**
     * get a value from the cache
     *
     *
     * @return T - the value retrieved from the cache, null if not found
     * @param key the id (string) of for the value
     */
    def get[T <:AnyRef](key: String)(implicit manifest : Manifest[T]):Option[T]

    /**
     * get a value from the cache from a group
     *
     * @return T - the value retrieved from the cache, null if not found
     * @param parentKey the id of the group
     * @param key the id (string) of for the value
     */
     def get[T <:AnyRef](parentKey: String,key: String)(implicit manifest : Manifest[T]):Option[T]
}
