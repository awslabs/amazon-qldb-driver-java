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
import com.amazonaws.services.qldbsession.model.CommitTransactionResult;
import com.amazonaws.services.qldbsession.model.ExecuteStatementResult;
import com.amazonaws.services.qldbsession.model.InvalidSessionException;
import com.amazonaws.services.qldbsession.model.OccConflictException;
import com.amazonaws.services.qldbsession.model.Page;
import com.amazonaws.services.qldbsession.model.StartTransactionResult;
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

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class TestTransactionImpl {
    private static final IonSystem system = IonSystemBuilder.standard().build();
    private static final String txnId = "txnId";

    @Mock
    private Session mockSession;

    @Mock
    private StartTransactionResult mockStartTransaction;

    @Mock
    private CommitTransactionResult mockCommitTransaction;

    private TransactionImpl txn;

    private ByteBuffer testCommitDigest;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void init() {
        MockitoAnnotations.initMocks(this);
        Page page = new Page().withNextPageToken(null).withValues(Collections.emptyList());
        ExecuteStatementResult dummyResult = new ExecuteStatementResult().withFirstPage(page);
        Mockito.when(mockSession.sendExecute(ArgumentMatchers.anyString(), ArgumentMatchers.anyList(),
                ArgumentMatchers.anyString())).thenReturn(dummyResult);
        Mockito.when(mockSession.sendExecute(ArgumentMatchers.anyString(), ArgumentMatchers.anyList(),
                ArgumentMatchers.anyString())).thenReturn(dummyResult);
        Mockito.when(mockStartTransaction.getTransactionId()).thenReturn(txnId);
        Mockito.when(mockSession.sendStartTransaction()).thenReturn(mockStartTransaction);

        txn = new TransactionImpl(mockSession, txnId, 1, system, null);
    }

    @Test
    public void testConstructor() {
        Mockito.when(mockSession.sendStartTransaction()).thenThrow(new AmazonClientException(""))
                .thenReturn(mockStartTransaction);
        txn = new TransactionImpl(mockSession, txnId, 1, system, null);
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
    public void testAbortInvalidSession() {
        Mockito.when(mockSession.sendAbort()).thenThrow(new InvalidSessionException(""));

        thrown.expect(InvalidSessionException.class);

        try {
            txn.abort();
        } finally {
            Assert.assertTrue(txn.isClosed.get());
        }
    }

    @Test
    public void testCommit() {
        testExecute();
        testCommitDigest = ByteBuffer.wrap(txn.getTransactionHash().getQldbHash());
        Mockito.when(mockCommitTransaction.getCommitDigest()).thenReturn(testCommitDigest);
        Mockito.when(mockSession.sendCommit(ArgumentMatchers.anyString(), ArgumentMatchers.any()))
                .thenReturn(mockCommitTransaction);
        txn.commit();
        verify(mockSession, times(1)).sendCommit(txnId,
                ByteBuffer.wrap(txn.getTransactionHash().getQldbHash()));
    }

    @Test
    public void testCommitMismatchedDigest(){
        testExecute();
        byte[] mockBytes = new byte[0];
        testCommitDigest = ByteBuffer.wrap(mockBytes);
        Mockito.when(mockCommitTransaction.getCommitDigest()).thenReturn(testCommitDigest);
        Mockito.when(mockSession.sendCommit(ArgumentMatchers.anyString(), ArgumentMatchers.any()))
                .thenReturn(mockCommitTransaction);

        thrown.expect(IllegalStateException.class);

        txn.commit();
    }

    @Test
    public void testClose() {
        txn.close();
        Assert.assertTrue(txn.isClosed.get());
    }

    @Test
    public void testAutoCloseableWithInvalidSessionException() {
        Mockito.when(mockSession.sendExecute(ArgumentMatchers.anyString(), ArgumentMatchers.anyList(),
                ArgumentMatchers.anyString())).thenThrow(new InvalidSessionException(""));

        thrown.expect(InvalidSessionException.class);

        try (Transaction txn = new TransactionImpl(mockSession, txnId, 1, system, null)) {
            txn.execute("stmtQuery");
        } finally {
            Mockito.verify(mockSession, Mockito.times(1)).sendAbort();
        }
    }

    @Test
    public void testAbortAfterCommit() {
        testCommit();

        // This should be a no-op.
        txn.abort();
        verify(mockSession, Mockito.never()).sendAbort();
    }

    @Test
    public void testCommitAfterCommit() {
        testCommit();

        thrown.expect(IllegalStateException.class);

        txn.commit();
    }

    @Test
    public void testCommitInvalidSession() {
        Mockito.when(mockSession.sendCommit(ArgumentMatchers.anyString(), ArgumentMatchers.any()))
                .thenThrow(new InvalidSessionException(""));

        thrown.expect(InvalidSessionException.class);

        try {
            txn.commit();
        } finally {
            Assert.assertTrue(txn.isClosed.get());
        }
    }

    @Test
    public void testCommitExceptionAbortSuccessful() {
        Mockito.when(mockSession.sendCommit(ArgumentMatchers.anyString(), ArgumentMatchers.any()))
                .thenThrow(new AmazonClientException(""));
        thrown.expect(AmazonClientException.class);

        try {
            txn.commit();
        } finally {
            Assert.assertTrue(txn.isClosed.get());
        }
    }

    @Test
    public void testCommitExceptionAbortException() {
        final AmazonClientException ace1 = new AmazonClientException("1");
        final AmazonClientException ace2 = new AmazonClientException("2");

        Mockito.when(mockSession.sendCommit(ArgumentMatchers.anyString(), ArgumentMatchers.any())).thenThrow(ace1);
        Mockito.when(mockSession.sendAbort()).thenThrow(ace2);

        thrown.expect(AmazonClientException.class);

        try {
            txn.commit();
        } catch (AmazonClientException ace) {
            Assert.assertEquals(ace, ace1);
            throw ace;
        } finally {
            Assert.assertTrue(txn.isClosed.get());
        }
    }

    @Test
    public void testExecuteAfterCommit() {
        testCommit();

        thrown.expect(IllegalStateException.class);

        executeQuery("stmtQuery", 1);
    }

    @Test
    public void testExecuteWithParametersAfterCommit() {
        testCommit();

        thrown.expect(IllegalStateException.class);

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

    @Test
    public void testExecuteCommitExecute() {
        testCommit();

        thrown.expect(IllegalStateException.class);

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

    @Test
    public void testExecuteWithOccConflict() {
        final String query = "stmtQuery";
        Mockito.when(mockSession.sendExecute(ArgumentMatchers.anyString(), ArgumentMatchers.anyList(),
                ArgumentMatchers.anyString())).thenThrow(new OccConflictException(""));

        thrown.expect(OccConflictException.class);

        txn.execute(query);
    }

    @Test
    public void testExecuteParamsWithOccConflict() {
        final String query = "stmtQuery";
        final List<IonValue> params = Collections.singletonList(system.singleValue("myValue"));
        Mockito.when(mockSession.sendExecute(ArgumentMatchers.anyString(), ArgumentMatchers.anyList(),
                ArgumentMatchers.anyString())).thenThrow(new OccConflictException(""));

        thrown.expect(OccConflictException.class);

        txn.execute(query, params);
    }

    @Test
    public void testExecuteWithInvalidSession() {
        final String query = "stmtQuery";
        final List<IonValue> params = Collections.singletonList(system.singleValue("myValue"));
        Mockito.when(mockSession.sendExecute(ArgumentMatchers.anyString(), ArgumentMatchers.anyList(),
                ArgumentMatchers.anyString())).thenThrow(new InvalidSessionException(""));

        thrown.expect(InvalidSessionException.class);

        txn.execute(query);
    }

    private void executeQuery(String query, int numExecutes) {
        final Result result = txn.execute(query);
        Assert.assertNotNull(result);

        final ArgumentCaptor<String> queryCaptor = ArgumentCaptor.forClass(String.class);
        final ArgumentCaptor<List> paramCaptor = ArgumentCaptor.forClass(List.class);
        final ArgumentCaptor<String> idCaptor = ArgumentCaptor.forClass(String.class);
        verify(mockSession, times(numExecutes)).sendExecute(queryCaptor.capture(), paramCaptor.capture(),
                idCaptor.capture());
        Assert.assertEquals(query, queryCaptor.getValue());
        Assert.assertTrue(paramCaptor.getValue().isEmpty());
        Assert.assertEquals(txnId, idCaptor.getValue());
    }
}
