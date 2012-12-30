package com.nice.zoocache

/**
 * User: arnonrgo
 * Date: 12/30/12
 * Time: 9:44 PM
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
     * @param parent the id (string) of the group to remove
     */
    def removeAll(parent : String)

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
