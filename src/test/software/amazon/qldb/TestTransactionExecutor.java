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

import com.amazon.ion.IonSystem;
import com.amazon.ion.IonValue;
import com.amazon.ion.system.IonSystemBuilder;
import com.amazonaws.AmazonClientException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import software.amazon.qldb.exceptions.AbortException;

import java.util.Collections;
import java.util.List;

public class TestTransactionExecutor {
    private static final IonSystem system = IonSystemBuilder.standard().build();

    private TransactionExecutor txnExec;

    @Mock
    private Result mockResult;

    @Mock
    private Transaction mockTxn;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
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

        thrown.expect(AbortException.class);

        txnExec.abort();
    }

    @Test
    public void testExecuteNoParams() {
        final String query = "my query";
        final Result result = txnExec.execute(query);
        Assert.assertEquals(mockResult, result);

        final ArgumentCaptor<String> queryCaptor = ArgumentCaptor.forClass(String.class);
        Mockito.verify(mockTxn, Mockito.times(1)).execute(queryCaptor.capture());
        Assert.assertEquals(query, queryCaptor.getValue());
    }

    @Test
    public void testExecuteEmptyParams() {
        final String query = "my query";
        final List<IonValue> params = Collections.emptyList();
        final Result result = txnExec.execute(query, params);
        Assert.assertEquals(mockResult, result);

        final ArgumentCaptor<String> queryCaptor = ArgumentCaptor.forClass(String.class);
        final ArgumentCaptor<List<IonValue>> listCaptor = ArgumentCaptor.forClass(List.class);
        Mockito.verify(mockTxn, Mockito.times(1)).execute(queryCaptor.capture(), listCaptor.capture());
        Assert.assertEquals(query, queryCaptor.getValue());
        Assert.assertEquals(params, listCaptor.getValue());
    }

    @Test
    public void testExecuteOneParam() {
        final String query = "my query";
        final List<IonValue> params = Collections.singletonList(system.singleValue("myValue"));
        final Result result = txnExec.execute(query, params);
        Assert.assertEquals(mockResult, result);

        final ArgumentCaptor<String> queryCaptor = ArgumentCaptor.forClass(String.class);
        final ArgumentCaptor<List<IonValue>> listCaptor = ArgumentCaptor.forClass(List.class);
        Mockito.verify(mockTxn, Mockito.times(1)).execute(queryCaptor.capture(), listCaptor.capture());
        Assert.assertEquals(query, queryCaptor.getValue());
        Assert.assertEquals(params, listCaptor.getValue());
        Assert.assertEquals(params.size(), listCaptor.getValue().size());
        Assert.assertEquals(params.get(0), listCaptor.getValue().get(0));
    }

    @Test
    public void testGetTxnId() {
        final String value = txnExec.getTransactionId();
        Assert.assertEquals("txnId", value);
        Mockito.verify(mockTxn, Mockito.times(1)).getTransactionId();
    }
}
