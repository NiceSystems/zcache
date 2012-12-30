package com.nice.zoocache.tests;

import org.msgpack.annotation.Message;

import java.util.ArrayList;
import java.util.List;

/**
 * User: arnonrgo
 * Date: 12/30/12
 * Time: 3:53 PM
 */
@Message
public class TestClass{
    InnerTestClass value = new InnerTestClass();
    Long longValue;

    List<String> listStrings = new ArrayList<String>();
}
