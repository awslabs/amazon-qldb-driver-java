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

package software.amazon.qldbstreaming;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.amazon.ion.IonSystem;
import com.amazon.ion.IonValue;
import com.amazon.ion.system.IonSystemBuilder;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.awssdk.services.qldbsessionv2.model.CommitTransactionResult;
import software.amazon.awssdk.services.qldbsessionv2.model.ExecuteStatementResult;
import software.amazon.awssdk.services.qldbsessionv2.model.Page;
import software.amazon.awssdk.services.qldbsessionv2.model.RateExceededException;
import software.amazon.awssdk.services.qldbsessionv2.model.ResultStream;
import software.amazon.awssdk.services.qldbsessionv2.model.StartTransactionResult;
import software.amazon.awssdk.services.qldbsessionv2.model.TransactionError;
import software.amazon.qldbstreaming.exceptions.TransactionException;

public class TransactionTest {
    private static final IonSystem system = IonSystemBuilder.standard().build();
    private static final String txnId = "txnId";

    @Mock
    private Session mockSession;

    @Mock
    private CompletableFuture<ResultStream> mockExecuteResultFuture;

    @Mock
    private CompletableFuture<ResultStream> mockCommitResultFuture;

    @Mock
    private CompletableFuture<ResultStream> mockAbortResultFuture;

    @Mock
    private StartTransactionResult mockStartTransaction;

    @Mock
    private CommitTransactionResult mockCommitTransaction;

    private Transaction txn;

    @BeforeEach
    public void init() throws InterruptedException, ExecutionException {
        MockitoAnnotations.initMocks(this);
        Mockito.when(mockSession.sendExecute(ArgumentMatchers.anyString(), ArgumentMatchers.anyList(),
            ArgumentMatchers.anyString())).thenReturn(mockExecuteResultFuture);
        Mockito.when(mockStartTransaction.transactionId()).thenReturn(txnId);
        Page page = Page.builder().nextPageToken(null).values(Collections.emptyList()).build();
        ExecuteStatementResult dummyResult = ExecuteStatementResult.builder().firstPage(page).build();
        Mockito.when(mockExecuteResultFuture.get()).thenReturn(dummyResult);
        Mockito.when(mockSession.sendAbort()).thenReturn(mockAbortResultFuture);
        Mockito.when(mockSession.sendCommit(txnId)).thenReturn(mockCommitResultFuture);
        Mockito.when(mockCommitResultFuture.get()).thenReturn(mockCommitTransaction);


        txn = new Transaction(mockSession, txnId, 1, system, null);
    }

    @Test
    public void testAbort() throws ExecutionException, InterruptedException {
        testExecute();

        txn.abort();
        verify(mockAbortResultFuture, times(1)).get();
    }

//    @Test
//    public void testAbortInvalidSession() {
//        Mockito.when(mockSession.sendAbort()).thenThrow(InvalidSessionException.builder().message("").build());
//
//        assertThrows(InvalidSessionException.class, () -> txn.abort());
//    }

    @Test
    public void testCommit() throws InterruptedException, ExecutionException {
        testExecute();
        txn.commit();
        verify(mockSession, times(1)).sendCommit(txnId);
    }

//    @Test
//    public void testCommitInvalidSession() {
//        Mockito.when(mockSession.sendCommit(ArgumentMatchers.anyString(), ArgumentMatchers.any()))
//               .thenThrow(InvalidSessionException.builder().message("").build());
//
//        assertThrows(InvalidSessionException.class, () -> txn.commit());
//    }

    @Test
    public void testCommitExceptionAbortSuccessful() throws InterruptedException, ExecutionException {
        Mockito.when(mockCommitResultFuture.get())
            .thenThrow(new ExecutionException(SdkServiceException.builder().message("").build()));
        assertThrows(SdkServiceException.class, () -> txn.commit());
    }

    @Test
    public void testCommitExceptionAbortException() throws InterruptedException, ExecutionException {
        final SdkClientException se1 = SdkClientException.builder().message("").build();
        final SdkClientException se2 = SdkClientException.builder().message("").build();
        Mockito.when(mockCommitResultFuture.get()).thenThrow(new ExecutionException(se1));
        Mockito.when(mockAbortResultFuture.get()).thenThrow(new ExecutionException(se2));

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
    public void testExecute() throws ExecutionException, InterruptedException {
        executeQuery("stmtQuery", 1);
    }

    @Test
    public void testExecuteExecute() throws ExecutionException, InterruptedException {
        executeQuery("stmtQuery", 1);
        executeQuery("stmtQuery2", 2);
    }

    @Test
    public void testExecuteWithParams() throws ExecutionException, InterruptedException {
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
    public void testExecuteWithTransactionError() throws InterruptedException {
        final String query = "stmtQuery";

        Mockito.when(mockSession.sendExecute(ArgumentMatchers.anyString(), ArgumentMatchers.anyList(),
            ArgumentMatchers.anyString())).thenThrow(TransactionException.create(TransactionError.builder().build()));

        assertThrows(TransactionException.class, () -> txn.execute(query));
    }

    @Test
    public void testExecuteWithRateExceeded() throws InterruptedException {
        final String query = "stmtQuery";

        Mockito.when(mockSession.sendExecute(ArgumentMatchers.anyString(), ArgumentMatchers.anyList(),
            ArgumentMatchers.anyString())).thenThrow(RateExceededException.builder().message("").build());

        assertThrows(RateExceededException.class, () -> txn.execute(query));
    }
//
//    @Test
//    public void testExecuteParamsWithOccConflict() {
//        final String query = "stmtQuery";
//        final List<IonValue> params = Collections.singletonList(system.singleValue("myValue"));
//        Mockito.when(mockSession.sendExecute(ArgumentMatchers.anyString(), ArgumentMatchers.anyList(),
//                                             ArgumentMatchers.anyString())).thenThrow(OccConflictException.builder().message("").build());
//
//        assertThrows(OccConflictException.class, () -> txn.execute(query, params));
//    }

//    @Test
//    public void testExecuteWithInvalidSession() {
//        final String query = "stmtQuery";
//        final List<IonValue> params = Collections.singletonList(system.singleValue("myValue"));
//        Mockito.when(mockSession.sendExecute(ArgumentMatchers.anyString(), ArgumentMatchers.anyList(),
//                                             ArgumentMatchers.anyString())).thenThrow(InvalidSessionException.builder().message("").build());
//
//        assertThrows(InvalidSessionException.class, () -> txn.execute(query));
//    }

    private void executeQuery(String query, int numExecutes) throws ExecutionException, InterruptedException {
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
