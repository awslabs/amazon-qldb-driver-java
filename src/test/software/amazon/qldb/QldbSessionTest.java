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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static software.amazon.qldb.MockResponses.SESSION_TOKEN;

import com.amazon.ion.IonSystem;
import com.amazon.ion.IonValue;
import com.amazon.ion.system.IonSystemBuilder;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.http.HttpStatus;
import org.apache.http.NoHttpResponseException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.awssdk.services.qldbsession.model.AbortTransactionRequest;
import software.amazon.awssdk.services.qldbsession.model.BadRequestException;
import software.amazon.awssdk.services.qldbsession.model.CapacityExceededException;
import software.amazon.awssdk.services.qldbsession.model.InvalidSessionException;
import software.amazon.awssdk.services.qldbsession.model.OccConflictException;
import software.amazon.awssdk.services.qldbsession.model.QldbSessionException;
import software.amazon.awssdk.services.qldbsession.model.RateExceededException;
import software.amazon.awssdk.services.qldbsession.model.SendCommandRequest;
import software.amazon.qldb.exceptions.TransactionAbortedException;

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
        client.queueResponse(MockResponses.START_SESSION_RESPONSE);
        mockSession = Session.startSession(LEDGER, client);
        qldbSession = new QldbSession(mockSession, READ_AHEAD, system, null);
    }

    @Test
    @DisplayName("close - SHOULD end session on QLDB WHEN closing the session")
    public void testClose() {
        client.queueResponse(MockResponses.endSessionResponse());
        qldbSession.close();
        assertTrue(qldbSession.isClosed());
    }

    @Test
    @DisplayName("close - SHOULD close the session WHEN QLDB throws an exception when ending the session")
    public void testEndSessionOnException() {
        client.queueResponse(SdkServiceException.builder().message("").build());

        qldbSession.close();
        assertTrue(qldbSession.isClosed());
    }

    @Test
    @DisplayName("isClosed - SHOULD return false by default WHEN session is open and there are no InvalidSessionException")
    public void testIsClosedWhenNotClosed() {
        assertFalse(qldbSession.isClosed());
    }

    @Test
    @DisplayName("execute - SHOULD close the session WHEN QLDB throws an InvalidSessionException")
    public void testSessionCloseOnInvalidSessionException() {
        final InvalidSessionException exception = InvalidSessionException.builder().message("").build();
        client.queueResponse(exception);
        assertThrows(InvalidSessionException.class, () -> {
            QldbSession qldbSession = null;
            try {
                qldbSession = new QldbSession(mockSession, READ_AHEAD, system, null);
                qldbSession.execute(txn -> null, null, new ExecutionContext());
            } finally {
                assertTrue(client.isQueueEmpty());
                assertTrue(qldbSession.isClosed());
            }
        });
    }

    @Test
    @DisplayName("execute - SHOULD not call the executor lambda WHEN QLDB fails to start a transaction")
    public void testStartTransactionError() {
        client.queueResponse(QldbSessionException.builder().message("an error").build());
        client.queueResponse(MockResponses.ABORT_RESPONSE);

        assertThrows(QldbSessionException.class, () -> {
            // StartTransaction is called at the beginning of the execute method
            qldbSession.execute(txn -> null, RetryPolicy.none(), new ExecutionContext());
        });
    }

    @Test
    @DisplayName("execute - SHOULD not call the executor lambda WHEN QLDB throws an InvalidSessionException when starting the "
                 + "transaction")
    public void testStartTransactionInvalidSessionException() {
        final InvalidSessionException exception = InvalidSessionException
            .builder()
            .message("msg")
            .build();
        client.queueResponse(exception);

        assertThrows(InvalidSessionException.class, () -> {
            // StartTransaction is called at the beginning of the execute method
            qldbSession.execute(txn -> null, RetryPolicy.none(), new ExecutionContext());
        });
    }

    @Test
    @DisplayName("execute - SHOULD retry WHEN QLDB executes a transaction but fails with 500 or 503 response status code "
                 + "but bubble up 404 responses ")
    public void testExecuteExecutorLambdaWithQldbSessionExceptions() throws IOException {
        final AwsServiceException exception1 = QldbSessionException.builder()
                                                                   .message("1")
                                                                   .statusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR)
                                                                   .build();
        // This exception should retry.
        queueTxnExecError(exception1);
        final AwsServiceException exception2 = QldbSessionException.builder()
                                                                   .message("2")
                                                                   .statusCode(HttpStatus.SC_SERVICE_UNAVAILABLE)
                                                                   .build();

        // This exception should retry.
        queueTxnExecError(exception2);
        final AwsServiceException exception3 = QldbSessionException.builder()
                                                                   .message("3")
                                                                   .statusCode(HttpStatus.SC_NOT_FOUND)
                                                                   .build();
        // This exception should throw.
        queueTxnExecError(exception3);

        try {
            assertThrows(QldbSessionException.class, () -> {
                qldbSession.execute(txnExecutor -> {
                    Result result = txnExecutor.execute(statement);
                    return result;
                }, retryPolicy, new ExecutionContext());
            });
        } finally {
            verify(retryPolicy, times(2)).backoffStrategy();
            verify(txnBackoff, times(1)).calculateDelay(argThat((RetryPolicyContext rpc) -> rpc.retriesAttempted() == 1));
            verify(txnBackoff, times(1)).calculateDelay(argThat((RetryPolicyContext rpc) -> rpc.retriesAttempted() == 2));
            verify(txnBackoff, never()).calculateDelay(argThat((RetryPolicyContext rpc) -> rpc.retriesAttempted() == 3));
        }
    }

    @Test
    @DisplayName("execute - SHOULD retry server side failures up to retry limit WHEN QLDB executes a transaction")
    public void testExecuteExecutorLambdaWithQldbSessionExceptionsExceedRetry() throws IOException {
        for (int i = 0; i < RETRY_LIMIT + 1; ++i) {
            final AwsServiceException exception = QldbSessionException
                .builder()
                .message("Error")
                .statusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR)
                .build();
            queueTxnExecError(exception);
        }

        try {
            assertThrows(QldbSessionException.class, () -> {
                qldbSession.execute(txnExecutor -> {
                    Result result = txnExecutor.execute(statement);
                    return result;
                }, retryPolicy, new ExecutionContext());
            });
        } finally {
            verify(retryPolicy, times(3)).backoffStrategy();
            verify(txnBackoff, times(1)).calculateDelay(argThat((RetryPolicyContext rpc) -> rpc.retriesAttempted() == 1));
            verify(txnBackoff, times(1)).calculateDelay(argThat((RetryPolicyContext rpc) -> rpc.retriesAttempted() == 2));
            verify(txnBackoff, times(1)).calculateDelay(argThat((RetryPolicyContext rpc) -> rpc.retriesAttempted() == 3));
            verify(txnBackoff, never()).calculateDelay(argThat((RetryPolicyContext rpc) -> rpc.retriesAttempted() == RETRY_LIMIT + 1));
        }
    }

    @Test
    @DisplayName("execute - SHOULD retry generic server side failures WHEN QLDB executes a transaction")
    public void testExecuteExecutorLambdaWithSdkServiceExceptions() throws IOException {
        // This exception should be retried.
        final SdkClientException exception1 = SdkClientException
            .builder()
            .message("Error 1")
            .cause(new NoHttpResponseException("cause"))
            .build();
        queueTxnExecError(exception1);

        // This exception should be retried.
        final SdkClientException exception2 = SdkClientException
            .builder()
            .message("Error 2")
            .cause(new SocketTimeoutException("cause"))
            .build();
        queueTxnExecError(exception2);

        // This exceptions should be thrown.
        final RateExceededException exception3 = RateExceededException.builder().message("3").build();
        queueTxnExecError(exception3);

        assertThrows(RateExceededException.class, () -> {
            try {
                qldbSession.execute(txnExecutor -> {
                    Result result = txnExecutor.execute(statement);
                    return result;
                }, retryPolicy, new ExecutionContext());
            } finally {
                verify(retryPolicy, times(2)).backoffStrategy();
                verify(txnBackoff, times(1)).calculateDelay(argThat((RetryPolicyContext rpc) -> rpc.retriesAttempted() == 1));
                verify(txnBackoff, times(1)).calculateDelay(argThat((RetryPolicyContext rpc) -> rpc.retriesAttempted() == 2));
                verify(txnBackoff, never()).calculateDelay(argThat((RetryPolicyContext rpc) -> rpc.retriesAttempted() == 3));
            }
        });
    }

    @Test
    @DisplayName("execute - SHOULD retry CapacityExceededException failures up to a limit WHEN QLDB executes a transaction")
    public void testExecuteExecutorLambdaWithCapacityExceededExceptionExceedRetry() throws IOException {
        for (int i = 0; i < RETRY_LIMIT + 1; ++i) {
            final CapacityExceededException exception = CapacityExceededException
                    .builder()
                    .statusCode(503)
                    .message("Capacity Exceeded Exception")
                    .build();
            queueTxnExecError(exception);
        }

        assertThrows(CapacityExceededException.class, () -> {
            try {
                qldbSession.execute(txnExecutor -> {
                    Result result = txnExecutor.execute(statement);
                    return result;
                }, retryPolicy, new ExecutionContext());
            } finally {
                verify(retryPolicy, times(3)).backoffStrategy();
                verify(txnBackoff, times(1)).calculateDelay(argThat((RetryPolicyContext rpc) -> rpc.retriesAttempted() == 1));
                verify(txnBackoff, times(1)).calculateDelay(argThat((RetryPolicyContext rpc) -> rpc.retriesAttempted() == 2));
                verify(txnBackoff, times(1)).calculateDelay(argThat((RetryPolicyContext rpc) -> rpc.retriesAttempted() == 3));
                verify(txnBackoff, never()).calculateDelay(argThat((RetryPolicyContext rpc) -> rpc.retriesAttempted() == RETRY_LIMIT + 1));
            }
        });
    }

    @Test
    @DisplayName("execute - SHOULD retry generic client failures up to a limit WHEN QLDB executes a transaction")
    public void testExecuteExecutorLambdaWithSdkClientExceptionExceedRetry() throws IOException {
        for (int i = 0; i < RETRY_LIMIT + 1; ++i) {
            final SdkClientException exception = SdkClientException
                .builder()
                .message("Error")
                .cause(new NoHttpResponseException("cause"))
                .build();
            queueTxnExecError(exception);
        }

        assertThrows(SdkClientException.class, () -> {
            try {
                qldbSession.execute(txnExecutor -> {
                    Result result = txnExecutor.execute(statement);
                    return result;
                }, retryPolicy, new ExecutionContext());
            } finally {
                verify(retryPolicy, times(3)).backoffStrategy();
                verify(txnBackoff, times(1)).calculateDelay(argThat((RetryPolicyContext rpc) -> rpc.retriesAttempted() == 1));
                verify(txnBackoff, times(1)).calculateDelay(argThat((RetryPolicyContext rpc) -> rpc.retriesAttempted() == 2));
                verify(txnBackoff, times(1)).calculateDelay(argThat((RetryPolicyContext rpc) -> rpc.retriesAttempted() == 3));
                verify(txnBackoff, never()).calculateDelay(argThat((RetryPolicyContext rpc) -> rpc.retriesAttempted() == RETRY_LIMIT + 1));
            }
        });
    }

    @Test
    @DisplayName("execute - SHOULD not retry WHEN starting a transaction but QLDB throws a InvalidSessionException")
    public void testExecuteStartTxnWithInvalidSessionException() {
        // GIVEN
        RetryPolicy retryPolicy = spy(RetryPolicy.none());
        qldbSession = new QldbSession(mockSession, READ_AHEAD, system, null);
        // Add an ISE when starting a txn
        final InvalidSessionException exception = InvalidSessionException.builder().message("").build();
        client.queueResponse(exception);

        // THEN
        assertThrows(InvalidSessionException.class, () -> {
            try {
                qldbSession.execute(txnExecutor -> {
                    Result result = txnExecutor.execute(statement);
                    return result;
                }, retryPolicy, new ExecutionContext());
            } finally {
                verify(retryPolicy, never()).backoffStrategy();
                assertTrue(qldbSession.isClosed());
            }
        });
    }

    @Test
    @DisplayName("execute - SHOULD bubble exception and not retry WHEN executing a statement QLDB throws a "
                 + "InvalidSessionException")
    // move this test
    public void testExecuteWithInvalidSessionException() throws IOException {
        // GIVEN
        RetryPolicy retryPolicy = spy(RetryPolicy.maxRetries(1));
        client = new MockQldbSessionClient();
        client.queueResponse(MockResponses.START_SESSION_RESPONSE);
        mockSession = Session.startSession(LEDGER, client);
        qldbSession = new QldbSession(mockSession, READ_AHEAD, system, null);

        // Add an ISE when executing a txn
        final InvalidSessionException exception = InvalidSessionException.builder().message("").build();
        queueTxnExecError(exception);

        // THEN
        assertThrows(InvalidSessionException.class, () -> {
            try {
                qldbSession.execute(txnExecutor -> {
                    Result result = txnExecutor.execute(statement);
                    return result;
                }, retryPolicy, new ExecutionContext());
            } finally {
                verify(retryPolicy, never()).backoffStrategy();
                assertTrue(qldbSession.isClosed());
            }
        });
    }

    @Test
    @DisplayName("execute with response - SHOULD return results WHEN QLDB executes a transaction")
    public void testExecuteExecutorLambdaWithReturnValueNoRetry() throws IOException {
        queueTxnExecCommit(ionList, statement, Collections.emptyList());

        final Result result = qldbSession.execute(txnExecutor -> {
            Result res = txnExecutor.execute(statement);
            return new BufferedResult(res);
        }, retryPolicy, new ExecutionContext());

        final Iterator<IonValue> resultIterator = result.iterator();
        final Iterator<IonValue> ionListIterator = ionList.iterator();
        compareIterators(ionListIterator, resultIterator);
    }

    @Test
    @DisplayName("execute - SHOULD retry up to retry limit WHEN QLDB throws OCC Errors")
    public void testExecuteExecutorLambdaWithReturnValueOccConflict() throws IOException {
        // Add one more error response than the number of configured OCC retries.

        for (int i = 0; i < RETRY_LIMIT + 1; ++i) {
            queueTxnExecError("id" + i);
        }
        try {
            assertThrows(QldbSessionException.class, () -> {
                qldbSession.execute(txnExecutor -> {
                    Result res = txnExecutor.execute(statement);
                    return new BufferedResult(res);
                }, retryPolicy, new ExecutionContext());
            });
        } finally {
            verify(retryPolicy, times(3)).backoffStrategy();
            verify(txnBackoff, times(1)).calculateDelay(argThat((RetryPolicyContext rpc) -> rpc.retriesAttempted() == 1));
            verify(txnBackoff, times(1)).calculateDelay(argThat((RetryPolicyContext rpc) -> rpc.retriesAttempted() == 2));
            verify(txnBackoff, times(1)).calculateDelay(argThat((RetryPolicyContext rpc) -> rpc.retriesAttempted() == 3));
            verify(txnBackoff, never()).calculateDelay(argThat((RetryPolicyContext rpc) -> rpc.retriesAttempted() == RETRY_LIMIT + 1));
        }
    }

    @Test
    @DisplayName("execute - SHOULD store results in memory WHEN executor returns results")
    public void testExecuteExecutorLambdaWithNonBufferedResultAndReturnValue() throws IOException {
        queueTxnExecCommit(ionList, statement, Collections.emptyList());

        final Result result = qldbSession.execute(txnExecutor -> {
            return txnExecutor.execute(statement);
        }, retryPolicy, new ExecutionContext());

        assertTrue(result instanceof BufferedResult);
        verify(retryPolicy, never()).backoffStrategy();
        final Iterator<IonValue> resultIterator = result.iterator();
        final Iterator<IonValue> ionListIterator = ionList.iterator();
        compareIterators(ionListIterator, resultIterator);
    }

    @Test
    @DisplayName("execute - SHOULD throw NullPointerException WHEN executor lambda is null")
    public void testInternalExecuteWithNullExecutor() throws IOException {
        queueTxnExecCommit(ionList, statement, Collections.emptyList());
        final Executor<Boolean> exec = null;

        assertThrows(NullPointerException.class, () -> {
            qldbSession.execute(exec, retryPolicy, new ExecutionContext());
        });
        verify(retryPolicy, never()).backoffStrategy();
    }

    @Test
    @DisplayName("execute - SHOULD close transaction WHEN transaction is aborted")
    public void testInternalExecuteWithAbortedTransaction() throws IOException {
        client = spy(new MockQldbSessionClient());
        client.queueResponse(MockResponses.START_SESSION_RESPONSE);
        mockSession = Session.startSession(LEDGER, client);
        qldbSession = new QldbSession(mockSession, READ_AHEAD, system, null);
        client.queueResponse(MockResponses.startTxnResponse("id"));
        client.queueResponse(MockResponses.executeResponse(ionList));
        client.queueResponse(MockResponses.ABORT_RESPONSE);

        ArgumentCaptor<SendCommandRequest> arg = ArgumentCaptor.forClass(SendCommandRequest.class);
        Mockito.verify(client).sendCommand(arg.capture());

        assertThrows(TransactionAbortedException.class, () -> {
            qldbSession.execute(txnExecutor -> {
                Result res = txnExecutor.execute(statement);
                BufferedResult bufferedResult = new BufferedResult(res);
                txnExecutor.abort();
                return bufferedResult;
            }, retryPolicy, new ExecutionContext());
        });
        verify(client, times(4)).sendCommand(any(SendCommandRequest.class));
        verify(retryPolicy, never()).backoffStrategy();
        final SendCommandRequest abortRequest =
                SendCommandRequest.builder().sessionToken(SESSION_TOKEN).abortTransaction(AbortTransactionRequest.builder().build()).build();
        verify(client, times(1)).sendCommand(
                eq(abortRequest));
    }

    @Test
    @DisplayName("execute - SHOULD retry WHEN starting a transaction QLDB throws a BadRequestException")
    public void testRetryExecuteWithTransactionAlreadyOpenException() throws IOException {
        client = spy(new MockQldbSessionClient());
        client.queueResponse(MockResponses.START_SESSION_RESPONSE);
        mockSession = Session.startSession(LEDGER, client);
        qldbSession = new QldbSession(mockSession, READ_AHEAD, system, null);

        final BadRequestException exception = BadRequestException.builder().message("Transaction already open").build();
        client.queueResponse(exception);
        client.queueResponse(MockResponses.ABORT_RESPONSE);

        // Then enqueue a set of successful operations to start, execute and commit the txn
        String txnId = "id";
        QldbHash txnHash = QldbHash.toQldbHash(txnId, system);
        txnHash = Transaction.dot(txnHash, statement, Collections.emptyList(), system);
        client.queueResponse(MockResponses.startTxnResponse(txnId));
        client.queueResponse(MockResponses.executeResponse(ionList));
        client.queueResponse(MockResponses.commitTransactionResponse(ByteBuffer.wrap(txnHash.getQldbHash())));

        qldbSession.execute(txnExecutor -> {
            return txnExecutor.execute(statement);
        }, retryPolicy, new ExecutionContext());

        verify(retryPolicy, times(1)).backoffStrategy();
        final SendCommandRequest abortRequest =
            SendCommandRequest.builder().sessionToken(SESSION_TOKEN).abortTransaction(AbortTransactionRequest.builder().build()).build();
        verify(client, times(1)).sendCommand(
             eq(abortRequest));
    }

    @Test
    @DisplayName("execute - SHOULD bubble up exception WHEN starting a transaction and retry attemtps exceeded")
    public void testExecuteWithTransactionAlreadyOpenExceptionAndRetryLimitExceeded() throws IOException {
        RetryPolicy retryPolicy = spy(RetryPolicy.none());

        qldbSession = new QldbSession(mockSession, READ_AHEAD, system, null);
        // BadRequestException on start transaction if the transaction somehow is already open
        final BadRequestException exception = BadRequestException.builder().message("Transaction already open").build();
        client.queueResponse(exception);
        client.queueResponse(MockResponses.ABORT_RESPONSE);

        assertThrows(BadRequestException.class, () -> {
            qldbSession.execute(txnExecutor -> {
                return txnExecutor.execute(statement);
            }, retryPolicy, new ExecutionContext());
        });

        verify(retryPolicy, never()).backoffStrategy();

    }

    @Test
    @DisplayName("execute - SHOULD close transaction WHEN QLDB fails to execute a statement")
    public void testInternalExecuteWithError() {
        client.queueResponse(MockResponses.startTxnResponse("id"));
        client.queueResponse(SdkClientException.builder().message("an Error1").build());
        client.queueResponse(MockResponses.ABORT_RESPONSE);

        assertThrows(SdkClientException.class, () -> {
            try {
                qldbSession.execute(txn -> {
                    return txn.execute(statement);
                }, RetryPolicy.none(), new ExecutionContext());
            } finally {
                assertTrue(client.isQueueEmpty());
            }
        });
    }

    @Test
    @DisplayName("execute - SHOULD close transaction WHEN an unknown exception is encountered")
    public void testInternalExecuteWithUnknownError() throws IOException {
        client = spy(new MockQldbSessionClient());
        client.queueResponse(MockResponses.START_SESSION_RESPONSE);
        mockSession = Session.startSession(LEDGER, client);
        qldbSession = new QldbSession(mockSession, READ_AHEAD, system, null);

        final RuntimeException exception = new RuntimeException("Unknown exception occurred");
        client.queueResponse(exception);
        client.queueResponse(MockResponses.ABORT_RESPONSE);

        // Then enqueue a set of successful operations to start, execute and commit the txn
        String txnId = "id";
        QldbHash txnHash = QldbHash.toQldbHash(txnId, system);
        txnHash = Transaction.dot(txnHash, statement, Collections.emptyList(), system);
        client.queueResponse(MockResponses.startTxnResponse(txnId));
        client.queueResponse(MockResponses.executeResponse(ionList));
        client.queueResponse(MockResponses.commitTransactionResponse(ByteBuffer.wrap(txnHash.getQldbHash())));

        assertThrows(RuntimeException.class, () -> {
            qldbSession.execute(txnExecutor -> {
                return txnExecutor.execute(statement);
            }, retryPolicy, new ExecutionContext());
        });

        final SendCommandRequest abortRequest =
                SendCommandRequest.builder().sessionToken(SESSION_TOKEN).abortTransaction(AbortTransactionRequest.builder().build()).build();
        verify(client, times(1)).sendCommand(
                eq(abortRequest));
    }

    @Test
    @DisplayName("execute - SHOULD not bubble up exception WHEN QLDB fails to abort transaction")
    public void testInternalExecuteWithErrorAndErrorOnAbort() {
        client.queueResponse(MockResponses.startTxnResponse("id"));
        final SdkClientException executionException = SdkClientException.builder().message("an Error1").build();
        client.queueResponse(executionException);
        client.queueResponse(SdkClientException.builder().message("an Error 2").build());

        try {
            qldbSession.execute(txn -> txn.execute(statement), RetryPolicy.none(), new ExecutionContext());
        }  catch(SdkClientException e) {
            assertTrue(executionException == e);
        } finally {
            assertTrue(client.isQueueEmpty());
        }
    }

    @Test
    @DisplayName("execute - SHOULD delay zero ms WHEN backoff strategy is null")
    public void testNullSleepTime() throws IOException {
        client.queueResponse(MockResponses.startTxnResponse("id"));
        client.queueResponse(SdkClientException.builder().message("an Error1").cause(new SocketTimeoutException()).build());
        client.queueResponse(MockResponses.ABORT_RESPONSE);

        queueTxnExecCommit(ionList, statement, Collections.emptyList());
        retryPolicy = new RetryPolicy(retryPolicyContext -> null, 1);

        assertDoesNotThrow(() -> {
            try {
                qldbSession.execute(txn -> txn.execute(statement), retryPolicy, new ExecutionContext());
            } finally {
                assertTrue(client.isQueueEmpty());
            }
        });
    }

    @Test
    @DisplayName("execute - SHOULD delay zero ms WHEN backoff strategy is negative")
    public void testNegativeSleepTime() throws IOException {
        client.queueResponse(MockResponses.startTxnResponse("id"));
        client.queueResponse(SdkClientException.builder().message("an Error1").cause(new SocketTimeoutException()).build());
        client.queueResponse(MockResponses.ABORT_RESPONSE);

        queueTxnExecCommit(ionList, statement, Collections.emptyList());
        retryPolicy = new RetryPolicy(retryPolicyContext -> Duration.ofMillis(-1), 1);

        assertDoesNotThrow(() -> {
            try {
                qldbSession.execute(txn -> txn.execute(statement), retryPolicy, new ExecutionContext());
            } finally {
                assertTrue(client.isQueueEmpty());
            }
        });
    }

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
        client.queueResponse(MockResponses.startTxnResponse(txnId));
        client.queueResponse(MockResponses.executeResponse(values));
        client.queueResponse(MockResponses.commitTransactionResponse(ByteBuffer.wrap(txnHash.getQldbHash())));
    }

    private void queueTxnExecError(String id) throws IOException {
        client.queueResponse(MockResponses.startTxnResponse(id));
        client.queueResponse(MockResponses.executeResponse(ionList));
        client.queueResponse(OccConflictException.builder().message("An OCC Exception").build());
    }

    public void queueTxnExecError(SdkException ace) throws IOException {
        client.queueResponse(MockResponses.startTxnResponse("id"));
        client.queueResponse(MockResponses.executeResponse(ionList));
        client.queueResponse(ace);
        client.queueResponse(MockResponses.ABORT_RESPONSE);
    }
}
