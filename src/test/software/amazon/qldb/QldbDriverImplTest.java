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

import org.apache.http.HttpStatus;
import org.apache.http.NoHttpResponseException;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.DisplayName;
import static org.mockito.ArgumentMatchers.eq;

import com.amazon.ion.IonSystem;
import com.amazon.ion.IonValue;
import com.amazon.ion.system.IonSystemBuilder;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.Mockito;
import static org.mockito.Mockito.*;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.qldbsession.QldbSessionClientBuilder;
import software.amazon.awssdk.services.qldbsession.model.CapacityExceededException;
import software.amazon.awssdk.services.qldbsession.model.InvalidSessionException;
import software.amazon.awssdk.services.qldbsession.model.OccConflictException;
import software.amazon.awssdk.services.qldbsession.model.QldbSessionException;
import software.amazon.awssdk.services.qldbsession.model.RateExceededException;
import software.amazon.qldb.exceptions.Errors;
import software.amazon.qldb.exceptions.QldbDriverException;

public class QldbDriverImplTest {
    private static final String LEDGER = "ledger";
    private static final int POOL_LIMIT = 2;
    private IonSystem system;
    private List<IonValue> ionList;
    private QldbDriver qldbDriverImpl;
    private String statement;
    private QldbDriverImpl retryDriver;

    private final MockQldbSessionClient mockClient = new MockQldbSessionClient();

    @Mock
    private QldbSessionClientBuilder mockBuilder;

    @Spy
    private RetryPolicy retryPolicy = RetryPolicy.maxRetries(3);

    @BeforeEach
    public void init() {
        MockitoAnnotations.initMocks(this);

        system = IonSystemBuilder.standard().build();
        ionList = new ArrayList<>(2);
        ionList.add(system.newString("a"));
        ionList.add(system.newString("b"));
        statement = "select * from test";

        Mockito.when(mockBuilder.build()).thenReturn(mockClient);

        qldbDriverImpl = QldbDriver.builder()
                                   .sessionClientBuilder(mockBuilder)
                                   .ledger(LEDGER)
                                   .ionSystem(system)
                                   .maxConcurrentTransactions(POOL_LIMIT)
                                   .transactionRetryPolicy(retryPolicy)
                                   .build();

        retryDriver = new QldbDriverImpl(LEDGER, mockClient, retryPolicy, 0, 50, IonSystemBuilder.standard().build(), null);
    }

    @Test
    public void testBuildWithNegativeMaxConcurrentTransactions() {
        assertThrows(IllegalArgumentException.class,
                     () -> QldbDriver.builder()
                                     .sessionClientBuilder(mockBuilder)
                                     .ledger(LEDGER)
                                     .maxConcurrentTransactions(-1)
                                     .build());
    }

    @Test
    public void testBuildWitNullRetryPolicy() {
        RetryPolicy retryPolicy = RetryPolicy.builder().maxRetries(3).build();
        assertThrows(NullPointerException.class,
                     () -> QldbDriver.builder()
                                     .sessionClientBuilder(mockBuilder)
                                     .ledger(LEDGER)
                                     .transactionRetryPolicy(null)
                                     .build());
    }

    @Test
    public void testBuildWitNullIonSystem() {
        assertThrows(NullPointerException.class,
                     () -> QldbDriver.builder()
                                     .sessionClientBuilder(mockBuilder)
                                     .ledger(LEDGER)
                                     .ionSystem(null)
                                     .build());
    }

    /**
     * Happy case
     *
     * @throws IOException
     */
    @Test
    public void testExecuteWithAvailableSession() throws IOException {
        mockClient.queueResponse(MockResponses.START_SESSION_RESPONSE);
        List<IonValue> parameters = Collections.emptyList();
        queueTxnExecCommit(ionList, statement, parameters);

        final Boolean returnedValue = qldbDriverImpl.execute(txn -> {
            txn.execute(statement, parameters);
            return true;
        });
        assertTrue(returnedValue);
    }

    /**
     * When a session in the pool throws InvalidSessionException, the driver goes to the next session
     * in the pool and executes the transaction
     *
     * @throws IOException
     */
    @Test
    public void testExecutePicksAnotherSessionWhenStartTransactionFails() throws IOException {
        mockClient.queueResponse(MockResponses.START_SESSION_RESPONSE);
        mockClient.queueResponse(InvalidSessionException.builder().message("Msg1").build());

        mockClient.queueResponse(MockResponses.START_SESSION_RESPONSE);
        List<IonValue> parameters = Collections.emptyList();
        queueTxnExecCommit(ionList, statement, parameters);


        QldbDriver qldbDriverImpl = QldbDriver.builder()
                                              .sessionClientBuilder(mockBuilder)
                                              .ledger(LEDGER)
                                              .maxConcurrentTransactions(2)
                                              .build();
        final Boolean result = qldbDriverImpl.execute(txn -> {
            txn.execute(statement, Collections.emptyList());
            return true;
        });
        assertTrue(mockClient.isQueueEmpty());
        assertTrue(result);
    }

    @ParameterizedTest
    @MethodSource("exceptionProvider")
    public void testExecuteCustomPolicy(SdkException exception) {
        RetryPolicy driverRetryPolicy = spy(RetryPolicy.maxRetries(3));
        qldbDriverImpl = QldbDriver.builder()
                                   .sessionClientBuilder(mockBuilder)
                                   .ledger(LEDGER)
                                   .maxConcurrentTransactions(POOL_LIMIT)
                                   .transactionRetryPolicy(driverRetryPolicy)
                                   .build();

        mockClient.queueResponse(MockResponses.START_SESSION_RESPONSE);
        mockClient.queueResponse(MockResponses.startTxnResponse("id"));
        mockClient.queueResponse(exception);
        mockClient.queueResponse(MockResponses.ABORT_RESPONSE);

        RetryPolicy customRetryPolicy = spy(RetryPolicy.none());
        assertThrows(exception.getClass(),
                     () -> {
             final Boolean result = qldbDriverImpl.execute(txn -> {
                 txn.execute(statement, Collections.emptyList());
                 return true;
             }, customRetryPolicy);
         });

        verify(driverRetryPolicy, never()).maxRetries();
        verify(customRetryPolicy, times(1)).maxRetries();
    }

    static Stream<SdkException> exceptionProvider () {
        return Stream.of(SdkClientException
                      .builder()
                      .message("Transient issue")
                      .cause(new SocketTimeoutException())
                      .build(),
                         OccConflictException.builder().message("Msg1").build()
                  );
    }


    /**
     * When a session in the pool throws InvalidSessionException, the driver goes to the next session
     * in the pool and executes the transaction
     *
     * @throws IOException
     */
    @Test
    public void testExecutePicksAnotherSessionWhenExecuteTransactionFails() throws IOException {
        mockClient.queueResponse(MockResponses.START_SESSION_RESPONSE);
        String txnId = "id";
        mockClient.queueResponse(MockResponses.startTxnResponse(txnId));
        mockClient.queueResponse(InvalidSessionException.builder().message("Msg1").build());

        mockClient.queueResponse(MockResponses.START_SESSION_RESPONSE);
        List<IonValue> parameters = Collections.emptyList();
        queueTxnExecCommit(ionList, statement, parameters);


        QldbDriver qldbDriverImpl = QldbDriver.builder()
                                              .sessionClientBuilder(mockBuilder)
                                              .ledger(LEDGER)
                                              .maxConcurrentTransactions(2)
                                              .build();
        final Boolean result = qldbDriverImpl.execute(txn -> {
            txn.execute(statement, Collections.emptyList());
            return true;
        }, retryPolicy);
        assertTrue(result);
        verify(retryPolicy, never()).backoffStrategy();
    }

    /**
     * Test the flavor of execute method which does not return anything and does
     * not take retryIndicator. This execute method eventually calls the main
     * execute method with the passed executor and retryIndicator as null
     */
    @Test
    public void testExecuteWithNoReturnNoRetry() throws IOException {
        mockClient.queueResponse(MockResponses.START_SESSION_RESPONSE);
        List<IonValue> parameters = Collections.emptyList();
        queueTxnExecCommit(ionList, statement, parameters);

        QldbDriver spyDriver = spy(qldbDriverImpl);
        ExecutorNoReturn executorNoReturn = (txn) -> txn.execute(statement, parameters);

        spyDriver.execute(executorNoReturn);
        verify(spyDriver).execute(eq(executorNoReturn), eq(retryPolicy));
    }

    /**
     * Test the flavor of execute method which does not return anything.
     * This execute method eventually calls the main execute method with the
     * passed executor and retryIndicator.
     */
    @Test
    public void testExecuteWithNoReturn() throws IOException {
        mockClient.queueResponse(MockResponses.START_SESSION_RESPONSE);
        List<IonValue> parameters = Collections.emptyList();
        queueTxnExecCommit(ionList, statement, parameters);

        QldbDriver spyDriver = spy(qldbDriverImpl);
        ExecutorNoReturn executorNoReturn = (txn) -> txn.execute(statement, parameters);

        spyDriver.execute(executorNoReturn, retryPolicy);
        verify(spyDriver).execute(eq(executorNoReturn), eq(retryPolicy));
    }

    @Test
    public void testExecuteStatementOccConflict() throws IOException {
        int retryLimit = 3;
        QldbDriver qldbDriverImpl = QldbDriver.builder()
                                              .sessionClientBuilder(mockBuilder)
                                              .ledger(LEDGER)
                                              .maxConcurrentTransactions(POOL_LIMIT)
                                              .transactionRetryPolicy(RetryPolicy.builder().maxRetries(retryLimit).build())
                                              .build();

        mockClient.queueResponse(MockResponses.START_SESSION_RESPONSE);
        // Add one more error response than the number of configured OCC retries.
        for (int i = 0; i < retryLimit + 1; ++i) {
            mockClient.queueResponse(MockResponses.startTxnResponse("id" + i));
            mockClient.queueResponse(MockResponses.executeResponse(ionList));
            mockClient.queueResponse(OccConflictException.builder().message("msg").build());
        }
        ExecutorNoReturn executorNoReturn = (txn) -> txn.execute(statement);

        assertThrows(OccConflictException.class, () ->
            qldbDriverImpl.execute(executorNoReturn, retryPolicy));
    }

    @Test
    public void testExecuteStatementTransactionExpired() throws IOException {
        int retryLimit = 3;
        String transactionExpiryMessage = "Transaction xyz has expired";
        QldbDriver qldbDriverImpl = QldbDriver.builder()
                .sessionClientBuilder(mockBuilder)
                .ledger(LEDGER)
                .maxConcurrentTransactions(POOL_LIMIT)
                .transactionRetryPolicy(RetryPolicy.builder().maxRetries(retryLimit).build())
                .build();

        mockClient.queueResponse(MockResponses.START_SESSION_RESPONSE);
        mockClient.queueResponse(MockResponses.startTxnResponse("id" + 0));
        mockClient.queueResponse(MockResponses.executeResponse(ionList));
        mockClient.queueResponse(InvalidSessionException.builder().message(transactionExpiryMessage).build());

        ExecutorNoReturn executorNoReturn = (txn) -> txn.execute(statement);
        assertThrows(InvalidSessionException.class, () ->
                qldbDriverImpl.execute(executorNoReturn, retryPolicy));
    }

    @Test
    public void testExecuteStatementTransactionNotExpired() throws IOException {
        int retryLimit = 3;
        String transactionExpiryMessage = "Session has expired";

        mockClient.queueResponse(MockResponses.START_SESSION_RESPONSE);
        String txnId = "id";
        mockClient.queueResponse(MockResponses.startTxnResponse(txnId));
        mockClient.queueResponse(InvalidSessionException.builder().message(transactionExpiryMessage).build());

        mockClient.queueResponse(MockResponses.START_SESSION_RESPONSE);
        List<IonValue> parameters = Collections.emptyList();
        queueTxnExecCommit(ionList, statement, parameters);


        QldbDriver qldbDriverImpl = QldbDriver.builder()
                .sessionClientBuilder(mockBuilder)
                .ledger(LEDGER)
                .maxConcurrentTransactions(POOL_LIMIT)
                .transactionRetryPolicy(RetryPolicy.builder().maxRetries(retryLimit).build())
                .build();

        final Boolean result = qldbDriverImpl.execute(txn -> {
            txn.execute(statement, Collections.emptyList());
            return true;
        }, retryPolicy);
        assertTrue(result);
    }

    @Test
    public void testExecuteWhenClosed() throws Exception {
        QldbDriver spyDriver = spy(qldbDriverImpl);
        spyDriver.close();

        ExecutorNoReturn executorNoReturn = (txn) -> txn.execute(statement);
        assertThrows(QldbDriverException.class, () ->
            spyDriver.execute(executorNoReturn, retryPolicy));
    }

    @Test
    public void testExecuteThenClose() throws Exception {
        QldbDriver spyDriver = spy(qldbDriverImpl);
        mockClient.startDummySession()
                  .addDummyTransaction("SELECT 1 FROM <<{}>>")
                  .addEndSession();

        spyDriver.execute(txn -> {
            txn.execute("SELECT 1 FROM <<{}>>");
        });
        spyDriver.close();

        assertTrue(mockClient.isQueueEmpty());
    }

    @Test
    public void testGetTableNames() throws IOException {
        final List<String> tables = Arrays.asList("table1", "table2");
        final List<IonValue> ionTables = tables.stream().map(system::newString).collect(Collectors.toList());

        mockClient.queueResponse(MockResponses.START_SESSION_RESPONSE);
        queueTxnExecCommit(ionTables, QldbDriverImpl.TABLE_NAME_QUERY, Collections.emptyList());

        final Iterable<String> result = qldbDriverImpl.getTableNames();
        final Iterator<String> resultIterator = result.iterator();
        final Iterator<String> tableIterator = tables.iterator();
        compareIterators(tableIterator, resultIterator);
    }

    @Test
    public void testGetTableNamesWhenClosed() throws Exception {
        qldbDriverImpl.close();
        assertThrows(QldbDriverException.class, () ->
            qldbDriverImpl.getTableNames());
    }

    @Test
    @DisplayName("execute - SHOULD delay zero ms WHEN backoff strategy is null")
    public void testNullSleepTime() throws IOException {
        mockClient.queueResponse(MockResponses.START_SESSION_RESPONSE);
        mockClient.queueResponse(MockResponses.startTxnResponse("id"));
        mockClient.queueResponse(SdkClientException.builder().message("an Error1").cause(new SocketTimeoutException()).build());
        mockClient.queueResponse(MockResponses.ABORT_RESPONSE);

        queueTxnExecCommit(ionList, statement, Collections.emptyList());
        RetryPolicy nullRetryPolicy = new RetryPolicy(retryPolicyContext -> null, 1);

        assertDoesNotThrow(() -> {
            try {
                retryDriver.execute(txn -> {
                    txn.execute(statement);
                }, nullRetryPolicy);
            } finally {
                assertTrue(mockClient.isQueueEmpty());
            }
        });
    }

    @Test
    @DisplayName("execute - SHOULD retry WHEN QLDB executes a transaction but fails with 500 or 503 response status code "
            + "but bubble up 404 responses ")
    public void testExecuteExecutorLambdaWithQldbSessionExceptions() throws IOException {
        BackoffStrategy txnBackoff = spy(new DefaultQldbTransactionBackoffStrategy());
        RetryPolicy retryPolicy = spy(RetryPolicy.builder()
                .maxRetries(3)
                .backoffStrategy(txnBackoff)
                .build());

        mockClient.queueResponse(MockResponses.START_SESSION_RESPONSE);
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

        assertThrows(QldbSessionException.class, () -> {
            try {
                retryDriver.execute(txnExecutor -> {
                    Result result = txnExecutor.execute(statement);
                    return result;
                }, retryPolicy);
            } finally {
                verify(retryPolicy, times(2)).backoffStrategy();
                verify(txnBackoff, times(1)).calculateDelay(argThat((RetryPolicyContext rpc) -> rpc.retriesAttempted() == 1));
                verify(txnBackoff, times(1)).calculateDelay(argThat((RetryPolicyContext rpc) -> rpc.retriesAttempted() == 2));
                verify(txnBackoff, never()).calculateDelay(argThat((RetryPolicyContext rpc) -> rpc.retriesAttempted() == 3));
            }
        });
    }

    @Test
    @DisplayName("execute - SHOULD retry up to retry limit WHEN QLDB throws OCC Errors")
    public void testExecuteExecutorLambdaWithReturnValueOccConflict() throws IOException {
        BackoffStrategy txnBackoff = spy(new DefaultQldbTransactionBackoffStrategy());
        RetryPolicy retryPolicy = spy(RetryPolicy.builder()
                .maxRetries(3)
                .backoffStrategy(txnBackoff)
                .build());

        mockClient.queueResponse(MockResponses.START_SESSION_RESPONSE);

        // Add one more error response than the number of configured OCC retries.
        int retryLimit = 3;

        for (int i = 0; i < retryLimit + 1; ++i) {
            queueTxnExecOccError("id" + i);
        }
        try {
            assertThrows(QldbSessionException.class, () -> {
                retryDriver.execute(txnExecutor -> {
                    Result res = txnExecutor.execute(statement);
                    return new BufferedResult(res);
                }, retryPolicy);
            });
        } finally {
            verify(retryPolicy, times(3)).backoffStrategy();
            verify(txnBackoff, times(1)).calculateDelay(argThat((RetryPolicyContext rpc) -> rpc.retriesAttempted() == 1));
            verify(txnBackoff, times(1)).calculateDelay(argThat((RetryPolicyContext rpc) -> rpc.retriesAttempted() == 2));
            verify(txnBackoff, times(1)).calculateDelay(argThat((RetryPolicyContext rpc) -> rpc.retriesAttempted() == 3));
            verify(txnBackoff, never()).calculateDelay(argThat((RetryPolicyContext rpc) -> rpc.retriesAttempted() == retryLimit + 1));
        }
    }

    @Test
    @DisplayName("execute - SHOULD retry generic server side failures WHEN QLDB executes a transaction")
    public void testExecuteExecutorLambdaWithSdkServiceExceptions() throws IOException {
        BackoffStrategy txnBackoff = spy(new DefaultQldbTransactionBackoffStrategy());
        RetryPolicy retryPolicy = spy(RetryPolicy.builder()
                .maxRetries(3)
                .backoffStrategy(txnBackoff)
                .build());

        mockClient.queueResponse(MockResponses.START_SESSION_RESPONSE);

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
                retryDriver.execute(txnExecutor -> {
                    Result result = txnExecutor.execute(statement);
                    return result;
                }, retryPolicy);
            } finally {
                verify(retryPolicy, times(2)).backoffStrategy();
                verify(txnBackoff, times(1)).calculateDelay(argThat((RetryPolicyContext rpc) -> rpc.retriesAttempted() == 1));
                verify(txnBackoff, times(1)).calculateDelay(argThat((RetryPolicyContext rpc) -> rpc.retriesAttempted() == 2));
                verify(txnBackoff, never()).calculateDelay(argThat((RetryPolicyContext rpc) -> rpc.retriesAttempted() == 3));
            }
        });
    }

    @Test
    @DisplayName("execute - SHOULD retry generic client failures up to a limit WHEN QLDB executes a transaction")
    public void testExecuteExecutorLambdaWithSdkClientExceptionExceedRetry() throws IOException {
        BackoffStrategy txnBackoff = spy(new DefaultQldbTransactionBackoffStrategy());
        RetryPolicy retryPolicy = spy(RetryPolicy.builder()
                .maxRetries(3)
                .backoffStrategy(txnBackoff)
                .build());

        mockClient.queueResponse(MockResponses.START_SESSION_RESPONSE);

        int retryLimit = 3;
        for (int i = 0; i < retryLimit + 1; ++i) {
            final SdkClientException exception = SdkClientException
                    .builder()
                    .message("Error")
                    .cause(new NoHttpResponseException("cause"))
                    .build();
            queueTxnExecError(exception);
        }

        assertThrows(SdkClientException.class, () -> {
            try {
                retryDriver.execute(txnExecutor -> {
                    Result result = txnExecutor.execute(statement);
                    return result;
                }, retryPolicy);
            } finally {
                verify(retryPolicy, times(3)).backoffStrategy();
                verify(txnBackoff, times(1)).calculateDelay(argThat((RetryPolicyContext rpc) -> rpc.retriesAttempted() == 1));
                verify(txnBackoff, times(1)).calculateDelay(argThat((RetryPolicyContext rpc) -> rpc.retriesAttempted() == 2));
                verify(txnBackoff, times(1)).calculateDelay(argThat((RetryPolicyContext rpc) -> rpc.retriesAttempted() == 3));
                verify(txnBackoff, never()).calculateDelay(argThat((RetryPolicyContext rpc) -> rpc.retriesAttempted() == retryLimit + 1));
            }
        });
    }

    @Test
    @DisplayName("execute - SHOULD retry server side failures up to retry limit WHEN QLDB executes a transaction")
    public void testExecuteExecutorLambdaWithQldbSessionExceptionsExceedRetry() throws IOException {
        BackoffStrategy txnBackoff = spy(new DefaultQldbTransactionBackoffStrategy());
        RetryPolicy retryPolicy = spy(RetryPolicy.builder()
                .maxRetries(3)
                .backoffStrategy(txnBackoff)
                .build());

        mockClient.queueResponse(MockResponses.START_SESSION_RESPONSE);

        int retryLimit = 3;
        for (int i = 0; i < retryLimit + 1; ++i) {
            final AwsServiceException exception = QldbSessionException
                    .builder()
                    .message("Error")
                    .statusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR)
                    .build();
            queueTxnExecError(exception);
        }

        try {
            assertThrows(QldbSessionException.class, () -> {
                retryDriver.execute(txnExecutor -> {
                    Result result = txnExecutor.execute(statement);
                    return result;
                }, retryPolicy);
            });
        } finally {
            verify(retryPolicy, times(3)).backoffStrategy();
            verify(txnBackoff, times(1)).calculateDelay(argThat((RetryPolicyContext rpc) -> rpc.retriesAttempted() == 1));
            verify(txnBackoff, times(1)).calculateDelay(argThat((RetryPolicyContext rpc) -> rpc.retriesAttempted() == 2));
            verify(txnBackoff, times(1)).calculateDelay(argThat((RetryPolicyContext rpc) -> rpc.retriesAttempted() == 3));
            verify(txnBackoff, never()).calculateDelay(argThat((RetryPolicyContext rpc) -> rpc.retriesAttempted() == retryLimit + 1));
        }
    }

    @Test
    @DisplayName("execute - SHOULD retry CapacityExceededException failures up to retry limit WHEN QLDB executes a transaction")
    public void testExecuteExecutorLambdaWithCapacityExceededExceptionExceedRetry() throws IOException {
        BackoffStrategy txnBackoff = spy(new DefaultQldbTransactionBackoffStrategy());
        RetryPolicy retryPolicy = spy(RetryPolicy.builder()
                .maxRetries(3)
                .backoffStrategy(txnBackoff)
                .build());

        mockClient.queueResponse(MockResponses.START_SESSION_RESPONSE);

        int retryLimit = 3;
        for (int i = 0; i < retryLimit + 1; ++i) {
            final CapacityExceededException exception = CapacityExceededException
                    .builder()
                    .statusCode(503)
                    .message("Capacity Exceeded Exception")
                    .build();
            queueTxnExecError(exception);
        }

        try {
            assertThrows(QldbSessionException.class, () -> {
                retryDriver.execute(txnExecutor -> {
                    Result result = txnExecutor.execute(statement);
                    return result;
                }, retryPolicy);
            });
        } finally {
            verify(retryPolicy, times(3)).backoffStrategy();
            verify(txnBackoff, times(1)).calculateDelay(argThat((RetryPolicyContext rpc) -> rpc.retriesAttempted() == 1));
            verify(txnBackoff, times(1)).calculateDelay(argThat((RetryPolicyContext rpc) -> rpc.retriesAttempted() == 2));
            verify(txnBackoff, times(1)).calculateDelay(argThat((RetryPolicyContext rpc) -> rpc.retriesAttempted() == 3));
            verify(txnBackoff, never()).calculateDelay(argThat((RetryPolicyContext rpc) -> rpc.retriesAttempted() == retryLimit + 1));
        }
    }

    @Test
    @DisplayName("execute - SHOULD throw NullPointerException WHEN executor lambda is null")
    public void testInternalExecuteWithNullExecutor() throws IOException {
        queueTxnExecCommit(ionList, statement, Collections.emptyList());
        final Executor<Boolean> exec = null;

        assertThrows(NullPointerException.class, () -> {
            retryDriver.execute(exec);
        });
        verify(retryPolicy, never()).backoffStrategy();
    }

    @Test
    @DisplayName("execute - SHOULD only release session once WHEN execute throws retryable exception with unsuccessful abort and retry policy throws exception")
    public void testReleaseSessionIsOnlyCalledOnce() throws IOException {
        RuntimeException runtimeException = new RuntimeException();

        //Set up mockRetryPolicy to throw exception.
        final RetryPolicy mockRetryPolicy = Mockito.mock(RetryPolicy.class);
        Mockito.doThrow(runtimeException).when(mockRetryPolicy).backoffStrategy();
        Mockito.when(mockRetryPolicy.maxRetries()).thenReturn(1);

        //Set up driver to have semaphore of size 1.
        qldbDriverImpl = QldbDriver.builder()
                .sessionClientBuilder(mockBuilder)
                .ledger(LEDGER)
                .ionSystem(system)
                .maxConcurrentTransactions(1)
                .transactionRetryPolicy(mockRetryPolicy)
                .build();

        final CapacityExceededException cce = CapacityExceededException
                .builder()
                .statusCode(503)
                .message("Capacity Exceeded Exception")
                .build();
        mockClient.queueResponse(MockResponses.START_SESSION_RESPONSE);
        mockClient.queueResponse(MockResponses.startTxnResponse("id"));
        mockClient.queueResponse(MockResponses.executeResponse(ionList));
        mockClient.queueResponse(cce);
        // Queue exception for abort request.
        mockClient.queueResponse(runtimeException);

        assertThrows(RuntimeException.class, () -> qldbDriverImpl.execute(txnExecutor -> {
            txnExecutor.execute(statement);
        }));

        mockClient.queueResponse(MockResponses.START_SESSION_RESPONSE);
        mockClient.queueResponse(MockResponses.startTxnResponse("id"));

        // Use nested driver.execute() and attempt to get two permits from pool.
        QldbDriverException exception = assertThrows(QldbDriverException.class, () -> qldbDriverImpl.execute(txnExecutor -> {
            qldbDriverImpl.execute(txnExecutor2 -> {
                        txnExecutor2.execute(statement);
                    });
            txnExecutor.execute(statement);
        }));

        assertEquals(exception.getMessage(), (Errors.NO_SESSION_AVAILABLE.get()));
    }

    private void queueTxnExecCommit(List<IonValue> values, String statement, List<IonValue> parameters) throws IOException {
        String txnId = "id";
        QldbHash txnHash = QldbHash.toQldbHash(txnId, system);
        txnHash = Transaction.dot(txnHash, statement, parameters, system);
        mockClient.queueResponse(MockResponses.startTxnResponse(txnId));
        mockClient.queueResponse(MockResponses.executeResponse(values));
        mockClient.queueResponse(MockResponses.commitTransactionResponse(ByteBuffer.wrap(txnHash.getQldbHash())));
    }

    public void queueTxnExecError(SdkException ace) throws IOException {
        mockClient.queueResponse(MockResponses.startTxnResponse("id"));
        mockClient.queueResponse(MockResponses.executeResponse(ionList));
        mockClient.queueResponse(ace);
        mockClient.queueResponse(MockResponses.ABORT_RESPONSE);
    }

    private void queueTxnExecOccError(String id) throws IOException {
        mockClient.queueResponse(MockResponses.startTxnResponse(id));
        mockClient.queueResponse(MockResponses.executeResponse(ionList));
        mockClient.queueResponse(OccConflictException.builder().message("An OCC Exception").build());
    }

    private static <E> void compareIterators(Iterator<E> iterator1, Iterator<E> iterator2) {
        while (iterator2.hasNext() || iterator1.hasNext()) {
            assertEquals(iterator2.hasNext(), iterator1.hasNext());
            assertEquals(iterator1.next(), iterator2.next());
        }
    }
}
