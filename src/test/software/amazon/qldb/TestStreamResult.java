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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.amazon.ion.IonSystem;
import com.amazon.ion.IonValue;
import com.amazon.ion.system.IonSystemBuilder;
import com.amazonaws.AmazonClientException;
import com.amazonaws.services.qldbsession.model.Page;
import com.amazonaws.services.qldbsession.model.ValueHolder;

public class TestStreamResult {
    private static final IonSystem SYSTEM = IonSystemBuilder.standard().build();
    private static final String STR = "foo";
    private static final String TXN_ID = "txnId";
    private static final String PAGE_TOKEN = "token";
    private static final int READ_AHEAD_BUFFER_COUNT = 0;
    private static final IonSystem ionSystem = IonSystemBuilder.standard().build();

    private List<ValueHolder> mockValues;

    @Mock
    private Session mockSession;

    @Mock
    private Page mockPage;

    @Before
    public void init() {
        MockitoAnnotations.initMocks(this);
        mockValues = new ArrayList<>();
        Mockito.when(mockPage.getValues()).thenReturn(mockValues);
        Mockito.when(mockPage.getNextPageToken()).thenReturn(PAGE_TOKEN);
    }

    @Test
    public void testIsEmpty() {
        Mockito.when(mockPage.getNextPageToken()).thenReturn(null);
        final StreamResult streamResult = new StreamResult(
                mockSession, mockPage, TXN_ID, READ_AHEAD_BUFFER_COUNT, ionSystem, null);

        Assert.assertTrue(streamResult.isEmpty());
    }

    @Test
    public void testIsEmptyWhenNotEmpty() throws IOException {
        mockValues = MockResponses.createByteValues(Collections.singletonList(SYSTEM.singleValue(STR)));
        Mockito.when(mockPage.getValues()).thenReturn(mockValues);
        final StreamResult streamResult = new StreamResult(
                mockSession, mockPage, TXN_ID, READ_AHEAD_BUFFER_COUNT, ionSystem, null);

        Assert.assertFalse(streamResult.isEmpty());
    }

    @Test
    public void testIteratorHasNext() throws IOException {
        mockValues = MockResponses.createByteValues(Collections.singletonList(SYSTEM.singleValue(STR)));
        Mockito.when(mockPage.getValues()).thenReturn(mockValues);
        final StreamResult streamResult = new StreamResult(
                mockSession, mockPage, TXN_ID, READ_AHEAD_BUFFER_COUNT, ionSystem, null);

        final Iterator<IonValue> itr = streamResult.iterator();
        Assert.assertTrue(itr.hasNext());
    }

    @Test(expected = IllegalStateException.class)
    public void testIteratorRetrieveTwice() {
        Mockito.when(mockPage.getNextPageToken()).thenReturn(null);
        final StreamResult streamResult = new StreamResult(
                mockSession, mockPage, TXN_ID, READ_AHEAD_BUFFER_COUNT, ionSystem, null);

        streamResult.iterator();
        streamResult.iterator();
    }

    @Test
    public void testIteratorHasNextWhenEmpty() {
        Mockito.when(mockPage.getNextPageToken()).thenReturn(null);
        final StreamResult streamResult = new StreamResult(
                mockSession, mockPage, TXN_ID, READ_AHEAD_BUFFER_COUNT, ionSystem, null);

        final Iterator<IonValue> itr = streamResult.iterator();
        Assert.assertFalse(itr.hasNext());
    }

    @Test
    public void testIteratorNextWithOneElement() throws IOException {
        mockValues = MockResponses.createByteValues(Collections.singletonList(SYSTEM.singleValue(STR)));
        Mockito.when(mockPage.getValues()).thenReturn(mockValues);
        final StreamResult streamResult = new StreamResult(
                mockSession, mockPage, TXN_ID, READ_AHEAD_BUFFER_COUNT, ionSystem, null);

        final Iterator<IonValue> itr = streamResult.iterator();
        final IonValue result = itr.next();
        final IonValue expectedString = SYSTEM.singleValue(STR);
        Assert.assertEquals(expectedString, result);
    }

    @Test(expected = NoSuchElementException.class)
    public void testIteratorNextWhenTerminal() {
        Mockito.when(mockPage.getNextPageToken()).thenReturn(null);
        final StreamResult streamResult = new StreamResult(
                mockSession, mockPage, TXN_ID, READ_AHEAD_BUFFER_COUNT, ionSystem, null);

        final Iterator<IonValue> itr = streamResult.iterator();
        itr.next();
    }

    @Test(expected = AmazonClientException.class)
    public void testIteratorNextRaisesException() throws IOException {
        final AmazonClientException exception = new AmazonClientException("mockMessage");
        mockValues = MockResponses.createByteValues(Collections.singletonList(SYSTEM.singleValue(STR)));
        Mockito.when(mockPage.getValues()).thenReturn(mockValues);
        Mockito.when(mockPage.getNextPageToken()).thenReturn(PAGE_TOKEN);
        Mockito.doThrow(exception).when(mockSession).sendFetchPage(ArgumentMatchers.anyString(), ArgumentMatchers.anyString());
        final StreamResult streamResult = new StreamResult(
                mockSession, mockPage, TXN_ID, READ_AHEAD_BUFFER_COUNT, ionSystem, null);

        final Iterator<IonValue> itr = streamResult.iterator();
        try {
            itr.next();
            itr.next();
        } catch (AmazonClientException e) {
            Assert.assertEquals(exception.getMessage(), e.getMessage());
            throw e;
        }
    }
}
