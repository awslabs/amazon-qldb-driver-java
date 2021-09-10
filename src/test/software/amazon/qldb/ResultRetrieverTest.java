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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.LinkedBlockingQueue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.awssdk.services.qldbsessionv2.model.FetchPageResult;
import software.amazon.awssdk.services.qldbsessionv2.model.Page;
import software.amazon.awssdk.services.qldbsessionv2.model.ValueHolder;
import software.amazon.qldb.exceptions.QldbDriverException;

public class ResultRetrieverTest {
    private static final int MOCK_READ_AHEAD = 2;
    private static final IonSystem SYSTEM = IonSystemBuilder.standard().build();
    private static final String MOCK_STRING = "foo";
    private static final String MOCK_TXN_ID = "txnId";
    private static final String MOCK_NEXT_PAGE_TOKEN = "nextPageToken";
    private static final IonValue MOCK_ION_VALUE = SYSTEM.singleValue(MOCK_STRING);
    private static final ByteBuffer MOCK_ION_BINARY = ByteBuffer.wrap(MOCK_STRING.getBytes());
    private static final ValueHolder MOCK_VALUE_HOLDER =
        ValueHolder.builder().ionBinary(SdkBytes.fromByteBuffer(MOCK_ION_BINARY)).build();
    private static final List<ValueHolder> MOCK_EMPTY_VALUES = new ArrayList<>();
    private static final List<ValueHolder> MOCK_VALUES = Collections.singletonList(MOCK_VALUE_HOLDER);
    private static final IonSystem ionSystem = IonSystemBuilder.standard().build();

    private ResultRetriever resultRetriever;

    @Mock
    private SessionV2 mockSession;

    @Mock
    private Page mockPage;

    @Mock
    private Page mockTerminalPage;

    @Mock
    private LinkedBlockingQueue<FetchPageResult> mockFetchPages;

    @Mock
    private FetchPageResult mockFetchPage;

    @Mock
    private software.amazon.awssdk.services.qldbsessionv2.model.IOUsage mockConsumedIOs;

    @Mock
    private software.amazon.awssdk.services.qldbsessionv2.model.TimingInformation mockSessionTimingInfo;

    @Mock
    private IOUsage mockIOUsage;

    @Mock
    private TimingInformation mockTimingInfo;

    @BeforeEach
    public void init() {
        MockitoAnnotations.initMocks(this);
        Mockito.when(mockPage.nextPageToken()).thenReturn(MOCK_NEXT_PAGE_TOKEN);
        Mockito.when(mockPage.values()).thenReturn(MOCK_EMPTY_VALUES);
        Mockito.when(mockTerminalPage.nextPageToken()).thenReturn(null);
        Mockito.when(mockTerminalPage.values()).thenReturn(MOCK_EMPTY_VALUES);
        Mockito.when(mockFetchPage.page()).thenReturn(mockTerminalPage);
        Mockito.when(mockSession.fetchPages(MOCK_NEXT_PAGE_TOKEN)).thenReturn(mockFetchPages);
        Mockito.when(mockFetchPages.poll()).thenReturn(mockFetchPage);
        Mockito.when(mockFetchPage.consumedIOs()).thenReturn(mockConsumedIOs);
        Mockito.when(mockFetchPage.timingInformation()).thenReturn(mockSessionTimingInfo);
    }

    private void initRetriever() {
        resultRetriever = new ResultRetriever(mockSession, mockPage, MOCK_TXN_ID, MOCK_READ_AHEAD,
                                              ionSystem, null, mockIOUsage, mockTimingInfo);
    }

    @Test
    public void testInitRaisesException() {
        final QldbDriverException exception = QldbDriverException.create("");
        Mockito.doThrow(exception).when(mockSession).fetchPages(MOCK_NEXT_PAGE_TOKEN);

        assertThrows(QldbDriverException.class, this::initRetriever);
    }

    @Test
    public void testClosedRetriever() {
        initRetriever();
        resultRetriever.close();

        // Fetch the next page while closed.
        assertThrows(QldbDriverException.class, () -> resultRetriever.next());
    }

    @Test
    public void testHasNext() {
        Mockito.when(mockPage.values()).thenReturn(MOCK_VALUES);
        initRetriever();
        assertTrue(resultRetriever.hasNext());
    }

    @Test
    public void testHasNextIsFalse() {
        Mockito.when(mockPage.nextPageToken()).thenReturn(null);
        initRetriever();

        assertFalse(resultRetriever.hasNext());
        Mockito.verify(mockPage, Mockito.times(2)).nextPageToken();
    }

    @Test
    public void testNext() {
        Mockito.when(mockPage.values()).thenReturn(MOCK_VALUES);
        initRetriever();

        assertEquals(MOCK_ION_VALUE, resultRetriever.next());
        Mockito.verify(mockPage, Mockito.times(2)).nextPageToken();
    }

    @Test
    public void testNextWhenTerminal() {
        Mockito.when(mockPage.nextPageToken()).thenReturn(null);
        initRetriever();

        assertThrows(NoSuchElementException.class, () -> {
            try {
                resultRetriever.next();
            } finally {
                Mockito.verify(mockPage, Mockito.times(2)).nextPageToken();
            }
        });
    }

    @Test
    public void testNextRaisesNoSuchElementException() {
        initRetriever();

        assertThrows(NoSuchElementException.class, () -> {
            try {
                resultRetriever.next();
            } finally {
                Mockito.verify(mockPage, Mockito.times(3)).nextPageToken();
            }
        });
    }

    @Test
    public void testGetNextResult() {
        Mockito.when(mockPage.nextPageToken()).thenReturn(MOCK_NEXT_PAGE_TOKEN);
        Mockito.when(mockPage.values()).thenReturn(MOCK_EMPTY_VALUES);
        Mockito.when(mockPage.values()).thenReturn(MOCK_VALUES);
        initRetriever();

        final IonValue result = resultRetriever.next();

        Mockito.verify(mockPage, Mockito.times(2)).nextPageToken();
        assertEquals(MOCK_ION_VALUE, result);
    }
}