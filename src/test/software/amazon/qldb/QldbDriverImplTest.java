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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

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
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.qldbsession.QldbSessionClientBuilder;
import software.amazon.awssdk.services.qldbsession.model.InvalidSessionException;
import software.amazon.awssdk.services.qldbsession.model.OccConflictException;
import software.amazon.qldb.exceptions.QldbDriverException;

public class QldbDriverImplTest {
    private static final String LEDGER = "ledger";
    private static final int POOL_LIMIT = 2;
    private static final int TIMEOUT = 30000;
    private IonSystem system;
    private List<IonValue> ionList;
    private QldbDriver qldbDriverImpl;
    private String statement;

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

    @Test
    public void testGettingASessionFailsThenBubbleUpException()  {
        mockClient.queueResponse(AwsServiceException.builder().message("Issue").build());


        QldbDriver qldbDriverImpl = QldbDriver.builder()
                                              .sessionClientBuilder(mockBuilder)
                                              .ledger(LEDGER)
                                              .maxConcurrentTransactions(2)
                                              .build();
        assertThrows(AwsServiceException.class,
            () -> qldbDriverImpl.execute(txn -> {})
        );

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

    private void queueTxnExecCommit(List<IonValue> values, String statement, List<IonValue> parameters) throws IOException {
        String txnId = "id";
        QldbHash txnHash = QldbHash.toQldbHash(txnId, system);
        txnHash = Transaction.dot(txnHash, statement, parameters, system);
        mockClient.queueResponse(MockResponses.startTxnResponse(txnId));
        mockClient.queueResponse(MockResponses.executeResponse(values));
        mockClient.queueResponse(MockResponses.commitTransactionResponse(ByteBuffer.wrap(txnHash.getQldbHash())));
    }

    private static <E> void compareIterators(Iterator<E> iterator1, Iterator<E> iterator2) {
        while (iterator2.hasNext() || iterator1.hasNext()) {
            assertEquals(iterator2.hasNext(), iterator1.hasNext());
            assertEquals(iterator1.next(), iterator2.next());
        }
    }
}
