package com.nice.tests;

import com.netflix.curator.test.TestingServer;
import com.nice.zoocache.ZooCache;
import org.junit.Test;

/**
 * User: arnonrgo
 * Date: 12/27/12
 * Time: 2:11 PM
 */

// Test Java interoperability
public class JavaCompatabilityTests  {
    TestingServer server;
    String testCluster;
    ZooCache cache;

    public JavaCompatabilityTests() throws Exception {
        server = new TestingServer();
        testCluster=server.getConnectString();
        cache = new ZooCache(testCluster, "javaTest",true);
    }


    @Test
    public void Can_use_zoocache_with_Strings(){

/*        String key="testString";
        cache.put(key,str, ZooCache.FOREVER());
        Option output=cache.get(key, TypeToManifest.javaType(String.class));

        assertEquals((String) output.get(), str);
  */
    }


}
