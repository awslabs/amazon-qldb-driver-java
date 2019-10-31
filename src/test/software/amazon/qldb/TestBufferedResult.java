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

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.amazon.ion.IonSystem;
import com.amazon.ion.IonValue;
import com.amazon.ion.system.IonSystemBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import software.amazon.qldb.BufferedResult;
import software.amazon.qldb.Result;

public class TestBufferedResult {
    private static final IonSystem system = IonSystemBuilder.standard().build();

    private final List<IonValue> bufferedValue = Collections.singletonList(system.singleValue("myValue"));

    @Mock
    private Result mockResult;

    @Mock
    private Result emptyMockResult;

    @Before
    public void init() {
        MockitoAnnotations.initMocks(this);
        Mockito.when(mockResult.iterator()).thenReturn(bufferedValue.iterator());
    }

    @Test
    public void testIsEmptyWhenEmpty() {
        Mockito.when(emptyMockResult.iterator()).thenReturn(Collections.emptyIterator());
        final BufferedResult bufferedResult = new BufferedResult(emptyMockResult);
        Assert.assertTrue(bufferedResult.isEmpty());
    }

    @Test
    public void testIsEmptyWhenNotEmpty() {
        final BufferedResult bufferedResult = new BufferedResult(mockResult);
        Assert.assertFalse(bufferedResult.isEmpty());
    }

    @Test
    public void testIteratorThatIsNotEmpty() {
        final List<IonValue> values = Collections.singletonList(system.singleValue("myValue"));
        iterateValues(values);
    }

    @Test
    public void testIteratorThatIsEmpty() {
        iterateValues(Collections.emptyList());
    }

    private void iterateValues(List<IonValue> values) {
        final Iterator<IonValue> localItr = values.iterator();
        final Result result = Mockito.mock(Result.class);
        Mockito.when(result.iterator()).thenReturn(values.iterator());
        final BufferedResult bufferedResult = new BufferedResult(result);
        final Iterator<IonValue> bufferedResultItr = bufferedResult.iterator();

        while (localItr.hasNext() || bufferedResultItr.hasNext()) {
            Assert.assertEquals(localItr.hasNext(), bufferedResultItr.hasNext());
            Assert.assertEquals(localItr.next(), bufferedResultItr.next());
        }
    }
}
