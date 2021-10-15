///*
// * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// *
// * Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with
// * the License. A copy of the License is located at
// *
// * http://www.apache.org/licenses/LICENSE-2.0
// *
// * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// * CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
// * and limitations under the License.
// */
//package software.amazon.qldb;
//
//import static org.junit.jupiter.api.Assertions.assertEquals;
//import static org.junit.jupiter.api.Assertions.assertNull;
//
//import org.junit.jupiter.api.Test;
//import org.mockito.Mockito;
//import software.amazon.awssdk.services.qldbsession.model.Page;
//
//public class ResultHolderTest {
//    @Test
//    public void testWithResultOnly() {
//        final Page result = Mockito.mock(Page.class);
//        final ResultHolder<String> holder = new ResultHolder<>(result);
//        assertNull(holder.getAssociatedValue());
//        assertEquals(result, holder.getResult());
//    }
//
//    @Test
//    public void testWithValueOnly() {
//        final String val = "myValue";
//        final ResultHolder<String> holder = new ResultHolder<>(val);
//        assertEquals(val, holder.getAssociatedValue());
//        assertNull(holder.getResult());
//    }
//
//    @Test
//    public void testWithResultAndValue() {
//        final Page result = Mockito.mock(Page.class);
//        final String val = "myValue";
//        final ResultHolder<String> holder = new ResultHolder<>(result, val);
//        assertEquals(val, holder.getAssociatedValue());
//        assertEquals(result, holder.getResult());
//    }
//
//    @Test
//    public void testToString() {
//        final Page result = Mockito.mock(Page.class);
//        final String val = "myValue";
//        final ResultHolder<String> holder = new ResultHolder<>(result, val);
//        assertEquals("ResultHolder(Result: " + result + ", AssociatedValue: " + val + ")", holder.toString());
//    }
//}
