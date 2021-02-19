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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.amazon.ion.IonSystem;
import com.amazon.ion.IonValue;
import com.amazon.ion.system.IonSystemBuilder;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.awssdk.services.qldbsession.model.CommitTransactionResult;
import software.amazon.awssdk.services.qldbsession.model.ExecuteStatementResult;
import software.amazon.awssdk.services.qldbsession.model.InvalidSessionException;
import software.amazon.awssdk.services.qldbsession.model.OccConflictException;
import software.amazon.awssdk.services.qldbsession.model.Page;
import software.amazon.awssdk.services.qldbsession.model.StartTransactionResult;

public class TransactionTest {
    private static final IonSystem system = IonSystemBuilder.standard().build();
    private static final String txnId = "txnId";

    @Mock
    private Session mockSession;

    @Mock
    private StartTransactionResult mockStartTransaction;

    @Mock
    private CommitTransactionResult mockCommitTransaction;

    private Transaction txn;

    private SdkBytes testCommitDigest;

    @BeforeEach
    public void init() {
        MockitoAnnotations.initMocks(this);
        Page page = Page.builder().nextPageToken(null).values(Collections.emptyList()).build();
        ExecuteStatementResult dummyResult = ExecuteStatementResult.builder().firstPage(page).build();
        Mockito.when(mockSession.sendExecute(ArgumentMatchers.anyString(), ArgumentMatchers.anyList(),
                                             ArgumentMatchers.anyString())).thenReturn(dummyResult);
        Mockito.when(mockSession.sendExecute(ArgumentMatchers.anyString(), ArgumentMatchers.anyList(),
                                             ArgumentMatchers.anyString())).thenReturn(dummyResult);
        Mockito.when(mockStartTransaction.transactionId()).thenReturn(txnId);
        Mockito.when(mockSession.sendStartTransaction()).thenReturn(mockStartTransaction);

        txn = new Transaction(mockSession, txnId, 1, system, null);
    }

    @Test
    public void testAbort() {
        testExecute();
        txn.abort();
        verify(mockSession, times(1)).sendAbort();
    }

    @Test
    public void testAbortInvalidSession() {
        Mockito.when(mockSession.sendAbort()).thenThrow(InvalidSessionException.builder().message("").build());

        assertThrows(InvalidSessionException.class, () -> txn.abort());
    }

    @Test
    public void testCommit() {
        testExecute();
        testCommitDigest = SdkBytes.fromByteBuffer(ByteBuffer.wrap(txn.getTransactionHash().getQldbHash()));
        Mockito.when(mockCommitTransaction.commitDigest()).thenReturn(testCommitDigest);
        Mockito.when(mockSession.sendCommit(ArgumentMatchers.anyString(), ArgumentMatchers.any()))
               .thenReturn(mockCommitTransaction);
        txn.commit();
        verify(mockSession, times(1)).sendCommit(txnId,
                                                 ByteBuffer.wrap(txn.getTransactionHash().getQldbHash()));
    }

    @Test
    public void testCommitMismatchedDigest() {
        testExecute();
        byte[] mockBytes = new byte[0];
        testCommitDigest = SdkBytes.fromByteBuffer(ByteBuffer.wrap(mockBytes));
        Mockito.when(mockCommitTransaction.commitDigest()).thenReturn(testCommitDigest);
        Mockito.when(mockSession.sendCommit(ArgumentMatchers.anyString(), ArgumentMatchers.any()))
               .thenReturn(mockCommitTransaction);

        assertThrows(IllegalStateException.class, () -> txn.commit());
    }

    @Test
    public void testCommitInvalidSession() {
        Mockito.when(mockSession.sendCommit(ArgumentMatchers.anyString(), ArgumentMatchers.any()))
               .thenThrow(InvalidSessionException.builder().message("").build());

        assertThrows(InvalidSessionException.class, () -> txn.commit());
    }

    @Test
    public void testCommitExceptionAbortSuccessful() {
        Mockito.when(mockSession.sendCommit(ArgumentMatchers.anyString(), ArgumentMatchers.any()))
               .thenThrow(SdkServiceException.builder().message("").build());
        assertThrows(SdkServiceException.class, () -> txn.commit());
    }

    @Test
    public void testCommitExceptionAbortException() {
        final SdkClientException se1 = SdkClientException.builder().message("").build();
        final SdkClientException se2 = SdkClientException.builder().message("").build();

        Mockito.when(mockSession.sendCommit(ArgumentMatchers.anyString(), ArgumentMatchers.any())).thenThrow(se1);
        Mockito.when(mockSession.sendAbort()).thenThrow(se2);

        assertThrows(SdkClientException.class, () -> {
            try {
                txn.commit();
            } catch (SdkClientException se) {
                assertEquals(se, se1);
                throw se;
            }
        });
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
    public void testExecuteWithParams() {
        final String query = "stmtQuery";
        final List<IonValue> params = Collections.singletonList(system.singleValue("myValue"));
        final Result result = txn.execute(query, params);
        assertNotNull(result);

        final ArgumentCaptor<String> queryCaptor = ArgumentCaptor.forClass(String.class);
        final ArgumentCaptor<List<IonValue>> listCaptor = ArgumentCaptor.forClass(List.class);
        final ArgumentCaptor<String> idCaptor = ArgumentCaptor.forClass(String.class);
        verify(mockSession, times(1)).sendExecute(queryCaptor.capture(),
                                                  listCaptor.capture(), idCaptor.capture());
        assertEquals(query, queryCaptor.getValue());
        assertEquals(params, listCaptor.getValue());
        assertEquals(params.size(), listCaptor.getValue().size());
        assertEquals(params.get(0), listCaptor.getValue().get(0));
        assertEquals(txnId, idCaptor.getValue());
    }

    @Test
    public void testExecuteWithOccConflict() {
        final String query = "stmtQuery";
        Mockito.when(mockSession.sendExecute(ArgumentMatchers.anyString(), ArgumentMatchers.anyList(),
                                             ArgumentMatchers.anyString())).thenThrow(OccConflictException.builder().message("").build());

        assertThrows(OccConflictException.class, () -> txn.execute(query));
    }

    @Test
    public void testExecuteParamsWithOccConflict() {
        final String query = "stmtQuery";
        final List<IonValue> params = Collections.singletonList(system.singleValue("myValue"));
        Mockito.when(mockSession.sendExecute(ArgumentMatchers.anyString(), ArgumentMatchers.anyList(),
                                             ArgumentMatchers.anyString())).thenThrow(OccConflictException.builder().message("").build());

        assertThrows(OccConflictException.class, () -> txn.execute(query, params));
    }

    @Test
    public void testExecuteWithInvalidSession() {
        final String query = "stmtQuery";
        final List<IonValue> params = Collections.singletonList(system.singleValue("myValue"));
        Mockito.when(mockSession.sendExecute(ArgumentMatchers.anyString(), ArgumentMatchers.anyList(),
                                             ArgumentMatchers.anyString())).thenThrow(InvalidSessionException.builder().message("").build());

        assertThrows(InvalidSessionException.class, () -> txn.execute(query));
    }

    private void executeQuery(String query, int numExecutes) {
        final Result result = txn.execute(query);
        assertNotNull(result);

        final ArgumentCaptor<String> queryCaptor = ArgumentCaptor.forClass(String.class);
        final ArgumentCaptor<List> paramCaptor = ArgumentCaptor.forClass(List.class);
        final ArgumentCaptor<String> idCaptor = ArgumentCaptor.forClass(String.class);
        verify(mockSession, times(numExecutes)).sendExecute(queryCaptor.capture(), paramCaptor.capture(),
                                                            idCaptor.capture());
        assertEquals(query, queryCaptor.getValue());
        assertTrue(paramCaptor.getValue().isEmpty());
        assertEquals(txnId, idCaptor.getValue());
    }
}
