/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with
 * the License. A copy of the License is located at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */
package software.amazon.qldb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.amazon.ion.IonSystem;
import com.amazon.ion.IonValue;
import com.amazon.ion.system.IonSystemBuilder;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.awssdk.services.qldbsession.model.Page;
import software.amazon.awssdk.services.qldbsession.model.ValueHolder;

public class StreamResultTest {
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

    @BeforeEach
    public void init() {
        MockitoAnnotations.initMocks(this);
        mockValues = new ArrayList<>();
        Mockito.when(mockPage.values()).thenReturn(mockValues);
        Mockito.when(mockPage.nextPageToken()).thenReturn(PAGE_TOKEN);
    }

    @Test
    public void testIsEmpty() {
        Mockito.when(mockPage.nextPageToken()).thenReturn(null);
        final StreamResult streamResult = new StreamResult(
            mockSession, mockPage, TXN_ID, READ_AHEAD_BUFFER_COUNT, ionSystem, null);

        assertTrue(streamResult.isEmpty());
    }

    @Test
    public void testIsEmptyWhenNotEmpty() throws IOException {
        mockValues = MockResponses.createByteValues(Collections.singletonList(SYSTEM.singleValue(STR)));
        Mockito.when(mockPage.values()).thenReturn(mockValues);
        final StreamResult streamResult = new StreamResult(
            mockSession, mockPage, TXN_ID, READ_AHEAD_BUFFER_COUNT, ionSystem, null);

        assertFalse(streamResult.isEmpty());
    }

    @Test
    public void testIteratorHasNext() throws IOException {
        mockValues = MockResponses.createByteValues(Collections.singletonList(SYSTEM.singleValue(STR)));
        Mockito.when(mockPage.values()).thenReturn(mockValues);
        final StreamResult streamResult = new StreamResult(
            mockSession, mockPage, TXN_ID, READ_AHEAD_BUFFER_COUNT, ionSystem, null);

        final Iterator<IonValue> itr = streamResult.iterator();
        assertTrue(itr.hasNext());
    }

    @Test
    public void testIteratorRetrieveTwice() {
        Mockito.when(mockPage.nextPageToken()).thenReturn(null);
        final StreamResult streamResult = new StreamResult(
            mockSession, mockPage, TXN_ID, READ_AHEAD_BUFFER_COUNT, ionSystem, null);

        streamResult.iterator();

        assertThrows(IllegalStateException.class, () -> streamResult.iterator());
    }

    @Test
    public void testIteratorHasNextWhenEmpty() {
        Mockito.when(mockPage.nextPageToken()).thenReturn(null);
        final StreamResult streamResult = new StreamResult(
            mockSession, mockPage, TXN_ID, READ_AHEAD_BUFFER_COUNT, ionSystem, null);

        final Iterator<IonValue> itr = streamResult.iterator();
        assertFalse(itr.hasNext());
    }

    @Test
    public void testIteratorNextWithOneElement() throws IOException {
        mockValues = MockResponses.createByteValues(Collections.singletonList(SYSTEM.singleValue(STR)));
        Mockito.when(mockPage.values()).thenReturn(mockValues);
        final StreamResult streamResult = new StreamResult(
            mockSession, mockPage, TXN_ID, READ_AHEAD_BUFFER_COUNT, ionSystem, null);

        final Iterator<IonValue> itr = streamResult.iterator();
        final IonValue result = itr.next();
        final IonValue expectedString = SYSTEM.singleValue(STR);
        assertEquals(expectedString, result);
    }

    @Test
    public void testIteratorNextWhenTerminal() {
        Mockito.when(mockPage.nextPageToken()).thenReturn(null);
        final StreamResult streamResult = new StreamResult(
            mockSession, mockPage, TXN_ID, READ_AHEAD_BUFFER_COUNT, ionSystem, null);

        final Iterator<IonValue> itr = streamResult.iterator();

        assertThrows(NoSuchElementException.class, () -> itr.next());
    }

    @Test
    public void testIteratorNextRaisesException() throws IOException {
        final SdkServiceException exception = SdkServiceException.builder().message("an Error").build();
        mockValues = MockResponses.createByteValues(Collections.singletonList(SYSTEM.singleValue(STR)));
        Mockito.when(mockPage.values()).thenReturn(mockValues);
        Mockito.when(mockPage.nextPageToken()).thenReturn(PAGE_TOKEN);
        Mockito.doThrow(exception).when(mockSession).sendFetchPage(ArgumentMatchers.anyString(), ArgumentMatchers.anyString());
        final StreamResult streamResult = new StreamResult(
            mockSession, mockPage, TXN_ID, READ_AHEAD_BUFFER_COUNT, ionSystem, null);

        final Iterator<IonValue> itr = streamResult.iterator();

        assertThrows(SdkServiceException.class, () -> {
            try {
                itr.next();
                itr.next();
            } catch (SdkServiceException e) {
                assertEquals(exception.getMessage(), e.getMessage());
                throw e;
            }
        });
    }
}
