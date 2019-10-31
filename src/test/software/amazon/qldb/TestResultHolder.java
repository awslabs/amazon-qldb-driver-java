/*
 * Copyright 2014-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with
 * the License. A copy of the License is located at
 *
 * http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */
package software.amazon.qldb;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.amazonaws.services.qldbsession.model.Page;

public class TestResultHolder {
    @Test
    public void testWithResultOnly() {
        final Page result = Mockito.mock(Page.class);
        final ResultHolder<String> holder = new ResultHolder<>(result);
        Assert.assertNull(holder.getAssociatedValue());
        Assert.assertEquals(result, holder.getResult());
    }

    @Test
    public void testWithValueOnly() {
        final String val = "myValue";
        final ResultHolder<String> holder = new ResultHolder<>(val);
        Assert.assertEquals(val, holder.getAssociatedValue());
        Assert.assertNull(holder.getResult());
    }

    @Test
    public void testWithResultAndValue() {
        final Page result = Mockito.mock(Page.class);
        final String val = "myValue";
        final ResultHolder<String> holder = new ResultHolder<>(result, val);
        Assert.assertEquals(val, holder.getAssociatedValue());
        Assert.assertEquals(result, holder.getResult());
    }

    @Test
    public void testToString() {
        final Page result = Mockito.mock(Page.class);
        final String val = "myValue";
        final ResultHolder<String> holder = new ResultHolder<>(result, val);
        Assert.assertEquals("ResultHolder(Result: " + result + ", AssociatedValue: " + val + ")", holder.toString());
    }
}
