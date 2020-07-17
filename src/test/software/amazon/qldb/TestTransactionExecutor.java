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
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.amazon.ion.IonSystem;
import com.amazon.ion.IonValue;
import com.amazon.ion.system.IonSystemBuilder;
import com.amazonaws.AmazonClientException;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import software.amazon.qldb.exceptions.AbortException;

public class TestTransactionExecutor {
    private static final IonSystem SYSTEM = IonSystemBuilder.standard().build();

    private TransactionExecutor txnExec;

    @Mock
    private Result mockResult;

    @Mock
    private Transaction mockTxn;

    @BeforeEach
    public void init() {
        MockitoAnnotations.initMocks(this);

        Mockito.when(mockTxn.getTransactionId()).thenReturn("txnId");
        Mockito.when(mockTxn.execute(ArgumentMatchers.anyString())).thenReturn(mockResult);
        Mockito.when(mockTxn.execute(ArgumentMatchers.anyString(), ArgumentMatchers.anyList())).thenReturn(mockResult);
        txnExec = new TransactionExecutor(mockTxn);
    }

    @Test
    public void testAbortThrows() {
        Mockito.doThrow(new AmazonClientException("")).when(mockTxn).abort();

        assertThrows(AbortException.class,
            () -> txnExec.abort());
    }

    @Test
    public void testExecuteNoParams() {
        final String query = "my query";
        final Result result = txnExec.execute(query);
        assertEquals(mockResult, result);

        final ArgumentCaptor<String> queryCaptor = ArgumentCaptor.forClass(String.class);
        Mockito.verify(mockTxn, Mockito.times(1)).execute(queryCaptor.capture());
        assertEquals(query, queryCaptor.getValue());
    }

    @Test
    public void testExecuteEmptyParams() {
        final String query = "my query";
        final List<IonValue> params = Collections.emptyList();
        final Result result = txnExec.execute(query, params);
        assertEquals(mockResult, result);

        final ArgumentCaptor<String> queryCaptor = ArgumentCaptor.forClass(String.class);
        final ArgumentCaptor<List<IonValue>> listCaptor = ArgumentCaptor.forClass(List.class);
        Mockito.verify(mockTxn, Mockito.times(1)).execute(queryCaptor.capture(), listCaptor.capture());
        assertEquals(query, queryCaptor.getValue());
        assertEquals(params, listCaptor.getValue());
    }

    @Test
    public void testExecuteOneParam() {
        final String query = "my query";
        final List<IonValue> params = Collections.singletonList(SYSTEM.singleValue("myValue"));
        final Result result = txnExec.execute(query, params);
        assertEquals(mockResult, result);

        final ArgumentCaptor<String> queryCaptor = ArgumentCaptor.forClass(String.class);
        final ArgumentCaptor<List<IonValue>> listCaptor = ArgumentCaptor.forClass(List.class);
        Mockito.verify(mockTxn, Mockito.times(1)).execute(queryCaptor.capture(), listCaptor.capture());
        assertEquals(query, queryCaptor.getValue());
        assertEquals(params, listCaptor.getValue());
        assertEquals(params.size(), listCaptor.getValue().size());
        assertEquals(params.get(0), listCaptor.getValue().get(0));
    }

    @Test
    public void testGetTxnId() {
        final String value = txnExec.getTransactionId();
        assertEquals("txnId", value);
        Mockito.verify(mockTxn, Mockito.times(1)).getTransactionId();
    }
}
