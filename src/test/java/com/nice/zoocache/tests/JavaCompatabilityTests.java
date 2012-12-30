package com.nice.zoocache.tests;

import com.netflix.curator.test.TestingServer;
import com.nice.zoocache.JZooCache;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * User: arnonrgo
 * Date: 12/27/12
 * Time: 2:11 PM
 */

// Test Java interoperability
public class JavaCompatabilityTests  {
    TestingServer server;
    String testCluster;
    JZooCache cache;

    public JavaCompatabilityTests() throws Exception {
        server = new TestingServer();
        testCluster=server.getConnectString();
        cache = new JZooCache(testCluster, "javaTest");
    }


    @Test
    public void Can_use_zoocache_with_Strings(){

        String str="blah";
        String key="testString";
        cache.put(key,str);
        String result=cache.get(key,String.class);
        System.out.print(result);

        assertEquals(str,result);
    }

    @Test
    public void Can_use_zoocache_with_a_Compound_Class(){
        TestClass cls=new TestClass();
        cls.value.intValue=5;
        cls.longValue=100L;
        String key="testClass";

        cache.put(key,cls);
        TestClass result=cache.get(key,TestClass.class);
        System.out.print(result);

        assertEquals(cls.longValue,result.longValue);
        assertEquals(cls.value.intValue,result.value.intValue);
    }


}
