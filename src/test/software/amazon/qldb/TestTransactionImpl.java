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

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

import com.amazon.ion.IonSystem;
import com.amazon.ion.IonValue;
import com.amazon.ion.system.IonSystemBuilder;
import com.amazonaws.AmazonClientException;
import com.amazonaws.services.qldbsession.model.ExecuteStatementResult;
import com.amazonaws.services.qldbsession.model.OccConflictException;
import com.amazonaws.services.qldbsession.model.Page;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class TestTransactionImpl {
    private static final IonSystem system = IonSystemBuilder.standard().build();
    private static final String txnId = "txnId";

    @Mock
    private Session mockSession;

    private QldbSessionImpl mockQldbSession;

    private ByteBuffer mockCommitResult;

    private TransactionImpl txn;

    @Before
    public void init() {
        MockitoAnnotations.initMocks(this);
        Page page = new Page().withNextPageToken(null).withValues(Collections.emptyList());
        ExecuteStatementResult dummyResult = new ExecuteStatementResult().withFirstPage(page);
        Mockito.when(
            mockSession.sendExecute(ArgumentMatchers.anyString(), ArgumentMatchers.anyList(), ArgumentMatchers.anyString()))
                .thenReturn(dummyResult);
        Mockito.when(mockSession.sendExecute(ArgumentMatchers.anyString(), ArgumentMatchers.anyList(),
                ArgumentMatchers.anyString())).thenReturn(dummyResult);
        Mockito.when(mockSession.sendStartTransaction()).thenReturn(txnId);
        mockQldbSession = new QldbSessionImpl(mockSession, 4, 1, system, null);

        txn = new TransactionImpl(mockQldbSession, txnId, 1, system, null);
    }

    @Test
    public void testConstructor() {
        Mockito.when(mockSession.sendStartTransaction()).thenThrow(new AmazonClientException("")).thenReturn(txnId);
        txn = new TransactionImpl(mockQldbSession, txnId, 1, system, null);
        final String value = txn.getTransactionId();
        Assert.assertNotNull(value);
        Assert.assertEquals(txnId, value);
        Assert.assertEquals(mockSession, txn.session);
    }

    @Test
    public void testAbort() {
        testExecute();
        txn.abort();
        verify(mockSession, times(1)).sendAbort();
    }

    @Test
    public void testCommit() {
        testExecute();
        mockCommitResult = ByteBuffer.wrap(txn.getTransactionHash().getQldbHash());
        Mockito.when(mockSession.sendCommit(ArgumentMatchers.anyString(), ArgumentMatchers.any())).thenReturn(mockCommitResult);
        txn.commit();
        verify(mockSession, times(1)).sendCommit(txnId,
                ByteBuffer.wrap(txn.getTransactionHash().getQldbHash()));
    }

    @Test(expected = IllegalStateException.class)
    public void testCommitMismatchedDigest(){
        testExecute();
        byte[] mockBytes = new byte[0];
        mockCommitResult = ByteBuffer.wrap(mockBytes);
        Mockito.when(mockSession.sendCommit(ArgumentMatchers.anyString(), ArgumentMatchers.any())).thenReturn(mockCommitResult);
        txn.commit();
    }

    @Test
    public void testClose() {
        txn.close();

        Assert.assertTrue(txn.isClosed.get());
    }

    @Test
    public void testAbortAfterCommit() {
        testCommit();

        // This should be a no-op.
        txn.abort();
        verify(mockSession, Mockito.never()).sendAbort();
    }

    @Test(expected = IllegalStateException.class)
    public void testCommitAfterCommit() {
        testCommit();
        txn.commit();
    }

    @Test(expected = IllegalStateException.class)
    public void testExecuteAfterCommit() {
        testCommit();
        executeQuery("stmtQuery", 1);
    }

    @Test(expected = IllegalStateException.class)
    public void testExecuteWithParametersAfterCommit() {
        testCommit();
        testExecuteWithParams();
    }

    @Test
    public void testExecute() {
        executeQuery("stmtQuery", 1);
    }

    @Test
    public void testExecuteExecute() {
        executeQuery("stmtQuery", 1);
        executeQuery("stmtQuery2", 2);
    }

    @Test(expected = IllegalStateException.class)
    public void testExecuteCommitExecute() {
        testCommit();
        executeQuery("stmtQuery2", 2);
    }

    @Test
    public void testExecuteWithParams() {
        final String query = "stmtQuery";
        final List<IonValue> params = Collections.singletonList(system.singleValue("myValue"));
        final Result result = txn.execute(query, params);
        Assert.assertNotNull(result);

        final ArgumentCaptor<String> queryCaptor = ArgumentCaptor.forClass(String.class);
        final ArgumentCaptor<List<IonValue>> listCaptor = ArgumentCaptor.forClass(List.class);
        final ArgumentCaptor<String> idCaptor = ArgumentCaptor.forClass(String.class);
        verify(mockSession, times(1)).sendExecute(queryCaptor.capture(),
                listCaptor.capture(), idCaptor.capture());
        Assert.assertEquals(query, queryCaptor.getValue());
        Assert.assertEquals(params, listCaptor.getValue());
        Assert.assertEquals(params.size(), listCaptor.getValue().size());
        Assert.assertEquals(params.get(0), listCaptor.getValue().get(0));
        Assert.assertEquals(txnId, idCaptor.getValue());
    }

    @Test(expected = OccConflictException.class)
    public void testExecuteWithOccConflict() {
        final String query = "stmtQuery";
        Mockito.when(mockSession.sendExecute(ArgumentMatchers.anyString(), ArgumentMatchers.anyList(),
                ArgumentMatchers.anyString())).thenThrow(new OccConflictException(""));
        txn.execute(query);
    }

    @Test(expected = OccConflictException.class)
    public void testExecuteParamsWithOccConflict() {
        final String query = "stmtQuery";
        final List<IonValue> params = Collections.singletonList(system.singleValue("myValue"));
        Mockito.when(mockSession.sendExecute(ArgumentMatchers.anyString(), ArgumentMatchers.anyList(),
                ArgumentMatchers.anyString())).thenThrow(new OccConflictException(""));

        txn.execute(query, params);
    }

    private void executeQuery(String query, int numExecutes) {
        final Result result = txn.execute(query);
        Assert.assertNotNull(result);

        final ArgumentCaptor<String> queryCaptor = ArgumentCaptor.forClass(String.class);
        final ArgumentCaptor<List> paramCaptor = ArgumentCaptor.forClass(List.class);
        final ArgumentCaptor<String> idCaptor = ArgumentCaptor.forClass(String.class);
        verify(mockSession, times(numExecutes)).sendExecute(queryCaptor.capture(), paramCaptor.capture(), idCaptor.capture());
        Assert.assertEquals(query, queryCaptor.getValue());
        Assert.assertTrue(paramCaptor.getValue().isEmpty());
        Assert.assertEquals(txnId, idCaptor.getValue());
    }
}
