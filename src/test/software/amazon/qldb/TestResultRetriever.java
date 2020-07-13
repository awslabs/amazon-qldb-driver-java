/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
import com.amazonaws.AmazonClientException;
import com.amazonaws.services.qldbsession.model.FetchPageResult;
import com.amazonaws.services.qldbsession.model.Page;
import com.amazonaws.services.qldbsession.model.ValueHolder;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import software.amazon.qldb.exceptions.QldbClientException;

public class TestResultRetriever {
    private static final int MOCK_READ_AHEAD = 2;
    private static final IonSystem SYSTEM = IonSystemBuilder.standard().build();
    private static final String MOCK_STRING = "foo";
    private static final String MOCK_TXN_ID = "txnId";
    private static final String MOCK_NEXT_PAGE_TOKEN = "nextPageToken";
    private static final IonValue MOCK_ION_VALUE = SYSTEM.singleValue(MOCK_STRING);
    private static final ByteBuffer MOCK_ION_BINARY = ByteBuffer.wrap(MOCK_STRING.getBytes());
    private static final ValueHolder MOCK_VALUE_HOLDER = new ValueHolder().withIonBinary(MOCK_ION_BINARY);
    private static final List<ValueHolder> MOCK_EMPTY_VALUES = new ArrayList<>();
    private static final List<ValueHolder> MOCK_VALUES = Collections.singletonList(MOCK_VALUE_HOLDER);
    private static final IonSystem ION_SYSTEM = IonSystemBuilder.standard().build();

    private ResultRetriever resultRetriever;

    @Mock
    private Session mockSession;

    @Mock
    private Page mockPage;

    @Mock
    private Page mockTerminalPage;

    @Mock
    private FetchPageResult mockFetchPage;

    @BeforeEach
    public void init() {
        MockitoAnnotations.initMocks(this);
        Mockito.when(mockPage.getNextPageToken()).thenReturn(MOCK_NEXT_PAGE_TOKEN);
        Mockito.when(mockPage.getValues()).thenReturn(MOCK_EMPTY_VALUES);
        Mockito.when(mockTerminalPage.getNextPageToken()).thenReturn(null);
        Mockito.when(mockTerminalPage.getValues()).thenReturn(MOCK_EMPTY_VALUES);
        Mockito.when(mockFetchPage.getPage()).thenReturn(mockTerminalPage);
        Mockito.when(mockSession.sendFetchPage(MOCK_TXN_ID, MOCK_NEXT_PAGE_TOKEN)).thenReturn(mockFetchPage);
    }

    private void initRetriever() {
        resultRetriever = new ResultRetriever(mockSession, mockPage, MOCK_TXN_ID, MOCK_READ_AHEAD,
                ION_SYSTEM, null);
    }

    @Test
    public void testClosedRetriever() {
        Mockito.when(mockSession.sendFetchPage(MOCK_TXN_ID, MOCK_NEXT_PAGE_TOKEN)).thenReturn(mockFetchPage);
        initRetriever();
        resultRetriever.close();

        // Fetch the next page while closed.
        assertThrows(QldbClientException.class,
            () -> resultRetriever.next());
    }

    @Test
    public void testHasNext() {
        Mockito.when(mockPage.getValues()).thenReturn(MOCK_VALUES);
        initRetriever();
        assertTrue(resultRetriever.hasNext());
    }

    @Test
    public void testHasNextIsFalse() {
        Mockito.when(mockPage.getNextPageToken()).thenReturn(null);
        initRetriever();

        assertFalse(resultRetriever.hasNext());
        Mockito.verify(mockPage, Mockito.times(2)).getNextPageToken();
    }

    @Test
    public void testNext() {
        Mockito.when(mockPage.getValues()).thenReturn(MOCK_VALUES);
        initRetriever();

        assertEquals(MOCK_ION_VALUE, resultRetriever.next());
        Mockito.verify(mockPage, Mockito.times(2)).getNextPageToken();
    }

    @Test
    public void testNextWhenTerminal() {
        Mockito.when(mockPage.getNextPageToken()).thenReturn(null);
        initRetriever();

        try {
            assertThrows(NoSuchElementException.class,
                () -> resultRetriever.next());
        } finally {
            Mockito.verify(mockPage, Mockito.times(2)).getNextPageToken();
        }
    }

    @Test
    public void testNextRaisesNoSuchElementException() {
        initRetriever();

        try {
            assertThrows(NoSuchElementException.class,
                () -> resultRetriever.next());
        } finally {
            Mockito.verify(mockPage, Mockito.times(3)).getNextPageToken();
        }
    }

    @Test
    public void testRunRaisesException() {
        final AmazonClientException exception = new AmazonClientException("");
        Mockito.doThrow(exception).when(mockSession).sendFetchPage(MOCK_TXN_ID, MOCK_NEXT_PAGE_TOKEN);
        initRetriever();

        try {
            assertThrows(RuntimeException.class,
                () -> resultRetriever.next());
        } catch (RuntimeException e) {
            assertEquals(exception.getMessage(), e.getMessage());
            throw e;
        }
    }

    @Test
    public void testGetNextResult() {
        Mockito.when(mockPage.getNextPageToken()).thenReturn(MOCK_NEXT_PAGE_TOKEN);
        Mockito.when(mockPage.getValues()).thenReturn(MOCK_EMPTY_VALUES);
        Mockito.when(mockPage.getValues()).thenReturn(MOCK_VALUES);
        initRetriever();

        final IonValue result = resultRetriever.next();

        Mockito.verify(mockPage, Mockito.times(2)).getNextPageToken();
        assertEquals(MOCK_ION_VALUE, result);
    }

    @Test
    public void testGetNextResultRaisesException() {
        final AmazonClientException exception = new AmazonClientException("");

        Mockito.doThrow(exception).when(mockSession).sendFetchPage(MOCK_TXN_ID, MOCK_NEXT_PAGE_TOKEN);
        initRetriever();

        Mockito.verify(mockPage, Mockito.times(2)).getNextPageToken();

        try {
            assertThrows(AmazonClientException.class,
                () -> resultRetriever.next());
        } catch (RuntimeException e) {
            assertEquals(exception.getMessage(), e.getMessage());
            throw e;
        }
    }
}
