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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static software.amazon.qldb.MockResponses.SESSION_TOKEN;

import com.amazon.ion.IonList;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonValue;
import com.amazon.ion.system.IonSystemBuilder;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

public class QldbSessionTest {
    private static final String LEDGER = "myLedger";
    private static final int RETRY_LIMIT = 3;
    private static final int READ_AHEAD = 0;
    private MockQldbSessionClient client;
    private QldbSession qldbSession;
    private IonSystem system;
    private List<IonValue> ionList;
    private List<IonValue> ionList2;
    private String statement;
    private SessionV2 mockSession;
    BackoffStrategy txnBackoff;

    private RetryPolicy retryPolicy;

    @BeforeEach
    public void init() {
        txnBackoff = spy(new DefaultQldbTransactionBackoffStrategy());

        retryPolicy = spy(RetryPolicy.builder()
                                 .maxRetries(RETRY_LIMIT)
                                 .backoffStrategy(txnBackoff)
                                 .build());

        system = IonSystemBuilder.standard().build();
        ionList = new ArrayList<>(2);
        ionList.add(system.newString("a"));
        ionList.add(system.newString("b"));

        ionList2 = new ArrayList<>(2);
        ionList2.add(system.newString("c"));
        ionList2.add(system.newString("d"));

        statement = "select * from test";

        client = new MockQldbSessionClient();
        mockSession = new SessionV2(LEDGER, client);
        mockSession.startConnection();
        qldbSession = new QldbSession(mockSession, READ_AHEAD, system, null);
    }

    @Test
    @DisplayName("execute with response - SHOULD return buffered results WHEN QLDB executes a transaction without FetchPage")
    public void testExecuteStatementBufferedResult() throws IOException {
        queueTxnExecCommitMultipleResults(ionList, statement, Collections.emptyList());

        Result bufferedResult = qldbSession.execute(txnExecutor -> txnExecutor.execute(statement));

        List<IonValue> expectedValues = new ArrayList<>();
        for (int i = 0; i < 4; i ++) {
            expectedValues.add(system.newString("a"));
            expectedValues.add(system.newString("b"));
        }
        assertEquals(expectedValues, bufferedResult.getCurrentValues());
    }

    @Test
    @DisplayName("execute with response - SHOULD return stream result WHEN QLDB executes a statement without FetchPage")
    public void testExecuteStatementStreamResult() throws IOException {
        queueTxnExecCommitMultipleResults(ionList, statement, Collections.emptyList());

        qldbSession.execute(txnExecutor -> {
            Result streamResult = txnExecutor.execute(statement);
            List<IonValue> expectedValues = new ArrayList<>();
            for (int i = 0; i < 4; i ++) {
                expectedValues.add(system.newString("a"));
                expectedValues.add(system.newString("b"));
            }
            assertEquals(expectedValues, streamResult.getCurrentValues());

            return streamResult;
        });
    }

    @Test
    @DisplayName("execute with response - SHOULD return stream result WHEN QLDB executes multiple statements without FetchPage")
    public void testMultipleExecuteStreamResult() throws IOException {
        queueTxnMultipleExecCommitMutipleResults(ionList, ionList2, statement, Collections.emptyList());
        List<IonValue> expectedValues1 = new ArrayList<>();
        for (int i = 0; i < 4; i ++) {
            expectedValues1.add(system.newString("a"));
            expectedValues1.add(system.newString("b"));
        }
        List<IonValue> expectedValues2 = new ArrayList<>();
        for (int i = 0; i < 4; i ++) {
            expectedValues2.add(system.newString("c"));
            expectedValues2.add(system.newString("d"));
        }

        qldbSession.execute(txnExecutor -> {
            Result streamResult1 = txnExecutor.execute(statement);
            Result streamResult2 = txnExecutor.execute(statement);

            assertEquals(expectedValues2, streamResult2.getCurrentValues());
            assertEquals(expectedValues1, streamResult1.getCurrentValues());
            return null;
        });
    }

    @Test
    @DisplayName("execute with response - SHOULD return buffered result WHEN QLDB executes multiple statements without FetchPage")
    public void testMultipleExecuteBufferedResult() throws IOException {
        queueTxnMultipleExecCommitMutipleResults(ionList, ionList2, statement, Collections.emptyList());
        List<IonValue> expectedValues = new ArrayList<>();
        for (int i = 0; i < 4; i ++) {
            expectedValues.add(system.newString("a"));
            expectedValues.add(system.newString("b"));
        }

        Result result = qldbSession.execute(txnExecutor -> {
            Result streamResult1 = txnExecutor.execute(statement);
            txnExecutor.execute(statement);

            return streamResult1;
        });
        assertEquals(expectedValues, result.getCurrentValues());
    }

//    @Test
//    @DisplayName("close - SHOULD end session on QLDB WHEN closing the session")
//    public void testClose() {
//        client.queueResult(MockResponses.endSessionResponse());
//        qldbSession.close();
//    }

//    @Test
//    @DisplayName("close - SHOULD close the session WHEN QLDB throws an exception when ending the session")
//    public void testEndSessionOnException() {
//        client.exception = SdkServiceException.builder().message("").build();
//        qldbSession.close();
//    }
//
//    @Test
//    @DisplayName("execute - SHOULD not call the executor lambda WHEN QLDB fails to start a transaction")
//    public void testStartTransactionError() {
//        client.exception = QldbSessionException.builder().message("an error").build();
//        client.queueResponse(MockResponses.ABORT_RESPONSE);
//
//        assertThrows(ExecuteException.class, () -> {
//            // StartTransaction is called at the beginning of the execute method
//            qldbSession.execute(txn -> null);
//        });
//    }

//    @Test
//    @DisplayName("execute - SHOULD not call the executor lambda WHEN QLDB throws an InvalidSessionException when starting the "
//                 + "transaction")
//    public void testStartTransactionInvalidSessionException() {
//        final InvalidSessionException exception = InvalidSessionException
//            .builder()
//            .message("msg")
//            .build();
//        client.queueResponse(exception);
//
//        assertThrows(InvalidSessionException.class, () -> {
//            // StartTransaction is called at the beginning of the execute method
//            try {
//                qldbSession.execute(txn -> null);
//            } catch (ExecuteException te) {
//                assertTrue(te.isInvalidSessionException());
//                assertTrue(te.isRetryable());
//                throw te.getCause();
//            }
//        });
//    }

//    @Test
//    @DisplayName("execute - SHOULD wrap exception for retry WHEN executing a statement QLDB throws a "
//                 + "InvalidSessionException")
//    public void testExecuteWithInvalidSessionException() throws IOException {
//        // GIVEN
//        RetryPolicy retryPolicy = spy(RetryPolicy.maxRetries(1));
//        client = new MockQldbSessionClient();
//        client.queueResult(MockResponses.START_SESSION_RESPONSE);
//        mockSession = Session.startSession(LEDGER, client);
//        qldbSession = new QldbSession(mockSession, READ_AHEAD, system, null);
//
//        // Add an ISE when executing a txn
//        final InvalidSessionException exception = InvalidSessionException.builder().message("").build();
//        queueTxnExecError(exception);
//
//        // THEN
//        assertThrows(InvalidSessionException.class, () -> {
//            try {
//                qldbSession.execute(txnExecutor -> {
//                    Result result = txnExecutor.execute(statement);
//                    return result;
//                });
//            } catch (ExecuteException te) {
//                assertTrue(te.isInvalidSessionException());
//                assertTrue(te.isRetryable());
//                throw te.getCause();
//            } finally {
//                verify(retryPolicy, never()).backoffStrategy();
//            }
//        });
//    }

    @Test
    @DisplayName("execute with response - SHOULD return results WHEN QLDB executes a transaction")
    public void testExecuteExecutorLambdaWithReturnValueNoRetry() throws IOException {
        queueTxnExecCommit(ionList, statement, Collections.emptyList());

        final Result result = qldbSession.execute(txnExecutor -> {
            Result res = txnExecutor.execute(statement);
            return new BufferedResult(res);
        });

        final Iterator<IonValue> resultIterator = result.iterator();
        final Iterator<IonValue> ionListIterator = ionList.iterator();
        compareIterators(ionListIterator, resultIterator);
    }

    @Test
    @DisplayName("execute - SHOULD store results in memory WHEN executor returns results")
    public void testExecuteExecutorLambdaWithNonBufferedResultAndReturnValue() throws IOException {
        queueTxnExecCommit(ionList, statement, Collections.emptyList());

        final Result result = qldbSession.execute(txnExecutor -> txnExecutor.execute(statement));

        assertTrue(result instanceof BufferedResult);
        verify(retryPolicy, never()).backoffStrategy();
        final Iterator<IonValue> resultIterator = result.iterator();
        final Iterator<IonValue> ionListIterator = ionList.iterator();
        compareIterators(ionListIterator, resultIterator);
    }

//    @Test
//    @DisplayName("execute - SHOULD close transaction WHEN transaction is aborted")
//    public void testInternalExecuteWithAbortedTransaction() throws IOException {
//        client = spy(new MockQldbSessionClient());
//        mockSession = new SessionV2(LEDGER, client);
//        mockSession.startConnection();
//        qldbSession = new QldbSession(mockSession, READ_AHEAD, system, null);
//        client.queueResult(MockResponses.startTxnResponse("id"));
//        client.queueResult(MockResponses.executeResponse(ionList));
//        client.queueResult(MockResponses.abortTxnResponse());
//
//        ArgumentCaptor<SendCommandRequest> arg = ArgumentCaptor.forClass(SendCommandRequest.class);
//        Mockito.verify(client).sendCommand(arg.capture());
//
//        assertThrows(TransactionAbortedException.class, () -> {
//            try {
//                qldbSession.execute(txnExecutor -> {
//                    Result res = txnExecutor.execute(statement);
//                    BufferedResult bufferedResult = new BufferedResult(res);
//                    txnExecutor.abort();
//                    return bufferedResult;
//                });
//            } catch (ExecuteException te) {
//                throw te.getCause();
//            }
//        });
//        verify(client, times(4)).sendCommand(any(SendCommandRequest.class));
//        verify(retryPolicy, never()).backoffStrategy();
//        final SendCommandRequest abortRequest =
//                SendCommandRequest.builder().sessionToken(SESSION_TOKEN).abortTransaction(AbortTransactionRequest.builder().build()).build();
//        verify(client, times(1)).sendCommand(
//                eq(abortRequest));
//    }

//    @Test
//    @DisplayName("execute - SHOULD close transaction WHEN QLDB fails to execute a statement")
//    public void testInternalExecuteWithError() {
//        client.queueResponse(MockResponses.startTxnResponse("id"));
//        client.queueResponse(SdkClientException.builder().message("an Error1").build());
//        client.queueResponse(MockResponses.ABORT_RESPONSE);
//
//        assertThrows(ExecuteException.class, () -> {
//            try {
//                qldbSession.execute(txn -> {
//                    return txn.execute(statement);
//                });
//            } finally {
//                assertTrue(client.isQueueEmpty());
//            }
//        });
//    }

//    @Test
//    @DisplayName("execute - SHOULD close transaction WHEN an unknown exception is encountered")
//    public void testInternalExecuteWithUnknownError() throws IOException {
//        client = spy(new MockQldbSessionClient());
//        client.queueResponse(MockResponses.START_SESSION_RESPONSE);
//        mockSession = Session.startSession(LEDGER, client);
//        qldbSession = new QldbSession(mockSession, READ_AHEAD, system, null);
//
//        final RuntimeException exception = new RuntimeException("Unknown exception occurred");
//        client.queueResponse(exception);
//        client.queueResponse(MockResponses.ABORT_RESPONSE);
//
//        // Then enqueue a set of successful operations to start, execute and commit the txn
//        String txnId = "id";
//        QldbHash txnHash = QldbHash.toQldbHash(txnId, system);
//        txnHash = Transaction.dot(txnHash, statement, Collections.emptyList(), system);
//        client.queueResponse(MockResponses.startTxnResponse(txnId));
//        client.queueResponse(MockResponses.executeResponse(ionList));
//        client.queueResponse(MockResponses.commitTransactionResponse(ByteBuffer.wrap(txnHash.getQldbHash())));
//
//        assertThrows(RuntimeException.class, () -> {
//            qldbSession.execute(txnExecutor -> {
//                return txnExecutor.execute(statement);
//            });
//        });
//
//        final SendCommandRequest abortRequest =
//                SendCommandRequest.builder().sessionToken(SESSION_TOKEN).abortTransaction(AbortTransactionRequest.builder().build()).build();
//        verify(client, times(1)).sendCommand(
//                eq(abortRequest));
//    }

//    @Test
//    @DisplayName("execute - SHOULD bubble up exception with failed abort flag WHEN QLDB fails to abort transaction")
//    public void testInternalExecuteWithErrorAndErrorOnAbort() {
//        client.queueResult(MockResponses.startTxnResponse("id"));
//        final SdkClientException executionException = SdkClientException.builder().message("an Error1").build();
//        client.queueResult(executionException);
//        client.queueResult(SdkClientException.builder().message("an Error 2").build());
//
//        try {
//            qldbSession.execute(txn -> txn.execute(statement));
//        } catch (ExecuteException e) {
//            assertFalse(e.isSessionAlive());
//        } finally {
//            assertTrue(client.isQueueEmpty());
//        }
//    }

    private static <E> void compareIterators(Iterator<E> iterator1, Iterator<E> iterator2) {
        while (iterator2.hasNext() || iterator1.hasNext()) {
            assertEquals(iterator2.hasNext(), iterator1.hasNext());
            assertEquals(iterator1.next(), iterator2.next());
        }
    }

    private void queueTxnExecCommit(List<IonValue> values, String statement, List<IonValue> parameters) throws IOException {
        String txnId = "id";
        QldbHash txnHash = QldbHash.toQldbHash(txnId, system);
        txnHash = Transaction.dot(txnHash, statement, parameters, system);
        client.queueResult(MockResponses.startTxnResponse(txnId));
        client.queueResult(MockResponses.executeResponse(values));
        client.queueResult(MockResponses.commitTransactionResponse(ByteBuffer.wrap(txnHash.getQldbHash())));
    }

    private void queueTxnExecCommitMultipleResults(List<IonValue> values, String statement, List<IonValue> parameters) throws IOException {
        String txnId = "id";
        QldbHash txnHash = QldbHash.toQldbHash(txnId, system);
        txnHash = Transaction.dot(txnHash, statement, parameters, system);
        client.queueResult(MockResponses.startTxnResponse(txnId));
        client.queueResult(MockResponses.executeResponseWithNextPageToken("nextPageToken", values));
        client.queueResult(MockResponses.fetchPageResponse(values));
        client.queueResult(MockResponses.fetchPageResponse( values));
        client.queueResult(MockResponses.fetchPageResponseWithOutNextPageToken(values));
        client.queueResult(MockResponses.commitTransactionResponse(ByteBuffer.wrap(txnHash.getQldbHash())));
    }

    private void queueTxnMultipleExecCommitMutipleResults(List<IonValue> values1, List<IonValue> values2, String statement, List<IonValue> parameters) throws IOException {
        String txnId = "id";
        QldbHash txnHash = QldbHash.toQldbHash(txnId, system);
        txnHash = Transaction.dot(txnHash, statement, parameters, system);
        client.queueResult(MockResponses.startTxnResponse(txnId));
        client.queueResult(MockResponses.executeResponseWithNextPageToken("nextPageToken1", values1));
        client.queueResult(MockResponses.fetchPageResponse( values1));
        client.queueResult(MockResponses.fetchPageResponse(values1));
        client.queueResult(MockResponses.fetchPageResponseWithOutNextPageToken(values1));

        txnHash = Transaction.dot(txnHash, statement, parameters, system);
        client.queueResult(MockResponses.executeResponseWithNextPageToken("nextPageToken2", values2));
        client.queueResult(MockResponses.fetchPageResponse(values2));
        client.queueResult(MockResponses.fetchPageResponse(values2));
        client.queueResult(MockResponses.fetchPageResponseWithOutNextPageToken(values2));

        client.queueResult(MockResponses.commitTransactionResponse(ByteBuffer.wrap(txnHash.getQldbHash())));
    }

//    public void queueTxnExecError(SdkException ace) throws IOException {
//        client.queueResult(MockResponses.startTxnResponse("id"));
//        client.queueResult(MockResponses.executeResponse(ionList));
//        client.queueResult(ace);
//        client.queueResult(MockResponses.ABORT_RESPONSE);
//    }
}
