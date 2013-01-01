## What is ZooCache
ZooCache is a simple cache implementation on top of  [ZooKeeper™](http://zookeeper.apache.org/)


## Main Features

1. A cache with ZooKeeper capabilities (fault tolerance, distributed, etc.)
2. TTL for items
3. Can use a local shadow (simple LRU cache) to save on network calls
4. Ability to invalidate local shadow even if TTL has not passed

The project is currently compiled and tested with Scala 2.9.1

## Sponsors
![NICE](http://www.nice.com/sites/all/themes/nice/logo.png)

[NICE Systems](http://www.nice.com/) (NASDAQ: NICE), is the worldwide leader of intent-based solutions that capture and analyze interactions and transactions, realize intent, and extract and leverage insights to deliver impact in real time.
## Contributors
[Arnon Rotem-Gal-Oz](http://arnon.me)
## Open Source Projects in Use
  * [curator](https://github.com/Netflix/curator) - Netflix Zookeeper client library.
  * [messagepack](http://msgpack.org/) - Fast binary serializer/deserialzer.
  * [Apache commons-collection](http://commons.apache.org/collections/) - the  client side shadow uses the commons-collection LRU cache
  * [grizzled-slf4j](http://software.clapper.org/grizzled-slf4j/) -Scala friendly wrapper for slf4j
  * [Akka](http://akka.io/) - Akka Actor based concurrency