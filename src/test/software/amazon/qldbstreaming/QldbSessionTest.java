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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.amazon.ion.IonSystem;
import com.amazon.ion.IonValue;
import com.amazon.ion.system.IonSystemBuilder;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.qldbsessionv2.model.SendCommandRequest;
import software.amazon.awssdk.services.qldbsessionv2.model.SendCommandResponseHandler;
import software.amazon.awssdk.services.qldbsessionv2.model.StatementError;
import software.amazon.awssdk.services.qldbsessionv2.model.TransactionError;
import software.amazon.qldbstreaming.exceptions.ExecuteException;
import software.amazon.qldbstreaming.exceptions.TransactionAbortedException;

public class QldbSessionTest {
    private static final String LEDGER = "myLedger";
    private static final int RETRY_LIMIT = 3;
    private static final int READ_AHEAD = 0;
    private MockQldbSessionClient client;
    private QldbSession qldbSession;
    private IonSystem system;
    private List<IonValue> ionList;
    private String statement;
    private Session mockSession;
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
        statement = "select * from test";

        client = new MockQldbSessionClient();
        client.queueResponse(MockResponses.SEND_COMMAND_RESPONSE);
    }

    @Test
    @DisplayName("close - SHOULD end session on QLDB WHEN closing the session")
    public void testClose() throws ExecutionException, InterruptedException {
        client.queueResponse(MockResponses.END_SESSION_RESULT);

        mockSession = Session.startSession(LEDGER, client);
        qldbSession = new QldbSession(mockSession, READ_AHEAD, system, null);
        qldbSession.close();
    }

    @Test
    @DisplayName("close - SHOULD close the session WHEN QLDB throws an exception when ending the session")
    public void testEndSessionOnException() throws ExecutionException, InterruptedException {
        client.queueResponse(TransactionError.builder().message("an error").build());

        mockSession = Session.startSession(LEDGER, client);
        qldbSession = new QldbSession(mockSession, READ_AHEAD, system, null);
        qldbSession.close();
    }

    @Test
    @DisplayName("execute - SHOULD not call the executor lambda WHEN QLDB fails to start a transaction")
    public void testStartTransactionError() throws ExecutionException, InterruptedException {
        client.queueResponse(MockResponses.transactionErrorResponse("Transaction Error", ""));
        client.queueResponse(MockResponses.ABORT_TRANSACTION_RESULT);

        mockSession = Session.startSession(LEDGER, client);
        qldbSession = new QldbSession(mockSession, READ_AHEAD, system, null);

        assertThrows(ExecuteException.class, () -> {
            // StartTransaction is called at the beginning of the execute method
            qldbSession.execute(txn -> null);
        });
    }

    // TODO: Uncomment until Relay exception model is finalized
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

    // TODO: Uncomment until Relay exception model is finalized
//    @Test
//    @DisplayName("execute - SHOULD wrap exception for retry WHEN executing a statement QLDB throws a "
//                 + "InvalidSessionException")
//    public void testExecuteWithInvalidSessionException() throws IOException {
//        // GIVEN
//        RetryPolicy retryPolicy = spy(RetryPolicy.maxRetries(1));
//        client = new MockQldbSessionClient();
//        client.queueResponse(MockResponses.SEND_COMMAND_RESPONSE);
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
    public void testExecuteExecutorLambdaWithReturnValueNoRetry() throws IOException, ExecutionException, InterruptedException {
        queueTxnExecCommit(ionList);

        mockSession = Session.startSession(LEDGER, client);
        qldbSession = new QldbSession(mockSession, READ_AHEAD, system, null);

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
    public void testExecuteExecutorLambdaWithNonBufferedResultAndReturnValue()
        throws IOException, ExecutionException, InterruptedException {
        queueTxnExecCommit(ionList);

        mockSession = Session.startSession(LEDGER, client);
        qldbSession = new QldbSession(mockSession, READ_AHEAD, system, null);

        final Result result = qldbSession.execute(txnExecutor -> txnExecutor.execute(statement));

        assertTrue(result instanceof BufferedResult);
        verify(retryPolicy, never()).backoffStrategy();
        final Iterator<IonValue> resultIterator = result.iterator();
        final Iterator<IonValue> ionListIterator = ionList.iterator();
        compareIterators(ionListIterator, resultIterator);
    }

    @Test
    @DisplayName("execute - SHOULD close transaction WHEN transaction is aborted")
    public void testInternalExecuteWithAbortedTransaction() throws IOException, ExecutionException, InterruptedException {
        client = spy(new MockQldbSessionClient());
        client.queueResponse(MockResponses.SEND_COMMAND_RESPONSE);
        client.queueResponse(MockResponses.startTxnResponse("id"));
        client.queueResponse(MockResponses.executeResponse(ionList));
        client.queueResponse(MockResponses.ABORT_TRANSACTION_RESULT);
        mockSession = spy(Session.startSession(LEDGER, client));
        qldbSession = new QldbSession(mockSession, READ_AHEAD, system, null);

        assertThrows(TransactionAbortedException.class, () -> {
            try {
                qldbSession.execute(txnExecutor -> {
                    Result res = txnExecutor.execute(statement);
                    BufferedResult bufferedResult = new BufferedResult(res);
                    txnExecutor.abort();
                    return bufferedResult;
                });
            } catch (ExecuteException te) {
                throw te.getCause();
            }
        });

        verify(client, times(1))
            .sendCommand(any(SendCommandRequest.class), any(Publisher.class), any(SendCommandResponseHandler.class));
        verify(mockSession).sendStartTransaction();
        verify(mockSession).sendExecute(any(String.class), any(List.class), any(String.class));
        verify(retryPolicy, never()).backoffStrategy();
    }

    @Test
    @DisplayName("execute - SHOULD close transaction WHEN QLDB fails to execute a statement")
    public void testInternalExecuteWithError() throws ExecutionException, InterruptedException {
        client.queueResponse(MockResponses.startTxnResponse("id"));
        client.queueResponse(StatementError.builder().message("an Error1").build());
        client.queueResponse(MockResponses.ABORT_TRANSACTION_RESULT);
        mockSession = spy(Session.startSession(LEDGER, client));
        qldbSession = new QldbSession(mockSession, READ_AHEAD, system, null);

        assertThrows(ExecuteException.class, () -> {
            try {
                qldbSession.execute(txn -> txn.execute(statement));
            } finally {
                assertTrue(client.isQueueEmpty());
            }
        });

        verify(mockSession).sendAbort();
    }

    @Test
    @DisplayName("execute - SHOULD close transaction WHEN an unknown exception is encountered")
    public void testInternalExecuteWithUnknownError() throws IOException, ExecutionException, InterruptedException {
        final RuntimeException exception = new RuntimeException("Unknown exception occurred");
        client.queueResponse(exception);
        client.queueResponse(MockResponses.ABORT_TRANSACTION_RESULT);

        // Then enqueue a set of successful operations to start, execute and commit the txn
        String txnId = "id";
        client.queueResponse(MockResponses.startTxnResponse(txnId));
        client.queueResponse(MockResponses.executeResponse(ionList));
        client.queueResponse(MockResponses.commitTransactionResponse(txnId));

        mockSession = spy(Session.startSession(LEDGER, client));
        qldbSession = new QldbSession(mockSession, READ_AHEAD, system, null);

        assertThrows(RuntimeException.class, () -> {
            qldbSession.execute(txnExecutor -> {
                return txnExecutor.execute(statement);
            });
        });

        verify(mockSession, times(1)).sendAbort();
    }

    @Test
    @DisplayName("execute - SHOULD bubble up exception with failed abort flag WHEN QLDB fails to abort transaction")
    public void testInternalExecuteWithErrorAndErrorOnAbort() throws ExecutionException, InterruptedException {
        client.queueResponse(MockResponses.startTxnResponse("id"));
        client.queueResponse(MockResponses.transactionErrorResponse("an Error1", ""));
        client.queueResponse(MockResponses.transactionErrorResponse("an Error2", ""));

        mockSession = Session.startSession(LEDGER, client);
        qldbSession = new QldbSession(mockSession, READ_AHEAD, system, null);

        try {
            qldbSession.execute(txn -> txn.execute(statement));
        } catch (ExecuteException e) {
            assertFalse(e.isSessionAlive());
        } finally {
            assertTrue(client.isQueueEmpty());
        }
    }

    private static <E> void compareIterators(Iterator<E> iterator1, Iterator<E> iterator2) {
        while (iterator2.hasNext() || iterator1.hasNext()) {
            assertEquals(iterator2.hasNext(), iterator1.hasNext());
            assertEquals(iterator1.next(), iterator2.next());
        }
    }

    private void queueTxnExecCommit(List<IonValue> values) throws IOException {
        String txnId = "id";
        client.queueResponse(MockResponses.startTxnResponse(txnId));
        client.queueResponse(MockResponses.executeResponse(values));
        client.queueResponse(MockResponses.commitTransactionResponse(txnId));
    }

    public void queueTxnExecError(SdkException ace) throws IOException {
        client.queueResponse(MockResponses.startTxnResponse("id"));
        client.queueResponse(MockResponses.executeResponse(ionList));
        client.queueResponse(ace);
        client.queueResponse(MockResponses.ABORT_TRANSACTION_RESULT);
    }
}
