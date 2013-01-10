package com.nice.zoocache.tests;
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

import com.netflix.curator.test.TestingServer;
import com.nice.zoocache.JZooCache;
import org.junit.Test;

import java.io.IOException;
import java.util.Date;

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
        cache = new JZooCache(testCluster, "javaTest",false);
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
        cls.listStrings.add("blah");
        String key="testClass";

        cache.put(key,cls);
        TestClass result=cache.get(key,TestClass.class);
        System.out.print(result);

        assertEquals(cls.longValue,result.longValue);
        assertEquals(cls.value.intValue,result.value.intValue);
        assertEquals(cls.listStrings.get(0),result.listStrings.get(0));
    }

    @Test
    public void can_use_a_really_complex_class() throws IOException {
        BaseRequest<TestPerson> personRequest=new BaseRequest<TestPerson>();
        TestPerson person=new TestPerson(1,"Arnon",new Date());
        Address adr=new Address();
        adr.setCountry("Israel");
        person.addAddress(adr);
        personRequest.setBaseRequestBody(person);


        cache.put("1",person);
        //byte[] ser=msgpack.write(personRequest);


        //BaseRequest<TestPerson> newPersonReq=msgpack.read(ser,BaseRequest.class);
        //TestPerson newPerson=newPersonReq.getBaseRequestBody();
        MyTestPerson newPerson= cache.get("1",MyTestPerson.class);
        System.out.println(newPerson.getIdToAddress());


        // Create templates for serializing/deserializing List and Map objects





    }


}
