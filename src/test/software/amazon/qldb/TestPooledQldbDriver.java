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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.times;

import com.amazon.ion.IonSystem;
import com.amazon.ion.IonValue;
import com.amazon.ion.system.IonSystemBuilder;
import com.amazonaws.AmazonClientException;
import com.amazonaws.ClientConfigurationFactory;
import com.amazonaws.services.qldbsession.AmazonQLDBSessionClientBuilder;
import com.amazonaws.services.qldbsession.model.InvalidSessionException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import software.amazon.qldb.exceptions.QldbClientException;

public class TestPooledQldbDriver {
    private static final String LEDGER = "ledger";
    private static final int POOL_LIMIT = 2;
    private static final int TIMEOUT = 30000;
    private List<IonValue> ionList;
    private PooledQldbDriver pooledQldbDriver;
    private String statement;

    private final MockQldbSessionClient mockClient = new MockQldbSessionClient();

    @Mock
    private AmazonQLDBSessionClientBuilder mockBuilder;

    @Mock
    private RetryIndicator mockRetryIndicator;

    @Mock
    private QldbSession mockQldbSession;

    @BeforeEach
    public void init() {
        MockitoAnnotations.initMocks(this);

        final IonSystem system = IonSystemBuilder.standard().build();
        ionList  = new ArrayList<>(2);
        ionList.add(system.newString("a"));
        ionList.add(system.newString("b"));
        statement = "SELECT * FROM test";

        Mockito.when(mockBuilder.build()).thenReturn(mockClient);
        Mockito.when(mockBuilder.getClientConfiguration()).thenReturn(new ClientConfigurationFactory().getConfig());

        pooledQldbDriver = PooledQldbDriver.builder()
                .withSessionClientBuilder(mockBuilder)
                .withLedger(LEDGER)
                .withPoolLimit(POOL_LIMIT)
                .build();
    }

    // This is a test for PooledQldbDriverBuilder.
    @Test
    public void testBuildWithPoolLimit() {
        PooledQldbDriver.builder()
                .withSessionClientBuilder(mockBuilder)
                .withLedger(LEDGER)
                .withPoolLimit(POOL_LIMIT)
                .build();
    }

    // This is a test for PooledQldbDriverBuilder.
    @Test
    public void testBuildWithDefaultPoolLimit() {
        final PooledQldbDriver driver = PooledQldbDriver.builder()
                .withSessionClientBuilder(mockBuilder)
                .withLedger(LEDGER)
                .withPoolTimeout(0)
                .build();

        // Default limit should be set to 50 - the default limit of the configuration.
        for (int i = 0; i < 50; i++) {
            mockClient.queueResponse(MockResponses.START_SESSION_RESPONSE);
            driver.getSession();
        }

        assertThrows(QldbClientException.class, driver::getSession);
    }

    // This is a test for PooledQldbDriverBuilder.
    @Test
    public void testBuildWithPoolLimitNegative() {
        assertThrows(IllegalArgumentException.class,
            () -> PooledQldbDriver.builder()
                    .withSessionClientBuilder(mockBuilder)
                    .withLedger(LEDGER)
                    .withPoolLimit(-1)
                    .build());
    }

    // This is a test for PooledQldbDriverBuilder.
    @Test
    public void testBuildWithPoolLimitGreaterThanConfigLimit() {
        // Default for the builder is 50.
        assertThrows(IllegalArgumentException.class,
            () -> PooledQldbDriver.builder()
                    .withSessionClientBuilder(mockBuilder)
                    .withLedger(LEDGER)
                    .withPoolLimit(51)
                    .build());
    }

    // This is a test for PooledQldbDriverBuilder.
    @Test
    public void testBuildWithTimeout() {
        PooledQldbDriver.builder()
                .withSessionClientBuilder(mockBuilder)
                .withLedger(LEDGER)
                .withPoolTimeout(TIMEOUT)
                .build();
    }

    // This is a test for PooledQldbDriverBuilder.
    @Test
    public void testBuildWithTimeoutNegative() {
        assertThrows(IllegalArgumentException.class,
            () -> PooledQldbDriver.builder()
                    .withSessionClientBuilder(mockBuilder)
                    .withLedger(LEDGER)
                    .withPoolTimeout(-1)
                    .build());
    }

    @Test
    public void testClose() {
        mockClient.queueResponse(MockResponses.START_SESSION_RESPONSE);
        mockClient.queueResponse(MockResponses.endSessionResponse());

        final PooledQldbDriver driver = PooledQldbDriver.builder()
                .withSessionClientBuilder(mockBuilder)
                .withLedger(LEDGER)
                .withPoolLimit(POOL_LIMIT)
                .build();
        try {
            driver.getSession().close();
            driver.close();
        } catch (IllegalStateException ise) {
            fail("IllegalStateException should not occur here.");
        }

        assertThrows(IllegalStateException.class, driver::getSession);
    }

    @Test
    public void testAutoClosableWithInvalidSessionException() {
        final InvalidSessionException exception = new InvalidSessionException("msg");
        mockClient.queueResponse(exception);

        try (PooledQldbDriver driver = PooledQldbDriver.builder()
                .withSessionClientBuilder(mockBuilder)
                .withLedger(LEDGER)
                .withPoolLimit(POOL_LIMIT)
                .build()) {

            assertThrows(InvalidSessionException.class, driver::getSession, "msg");
        } finally {
            assertTrue(mockClient.isQueueEmpty());
        }
    }

    @Test
    public void testExecuteWithStatement() {
        final PooledQldbDriver driverSpy = Mockito.spy(pooledQldbDriver);

        Mockito.doReturn(mockQldbSession).when(driverSpy).getSession();
        Mockito.when(mockQldbSession.execute(statement)).thenReturn(null);

        assertNull(driverSpy.execute(statement));
        Mockito.verify(mockQldbSession, times(1)).execute(statement);
    }

    @Test
    public void testExecuteWithStatementAndRetry() {
        final PooledQldbDriver driverSpy = Mockito.spy(pooledQldbDriver);

        Mockito.doReturn(mockQldbSession).when(driverSpy).getSession();
        Mockito.when(mockQldbSession.execute(statement, mockRetryIndicator)).thenReturn(null);

        assertNull(driverSpy.execute(statement, mockRetryIndicator));
        Mockito.verify(mockQldbSession, times(1)).execute(statement, mockRetryIndicator);
    }

    @Test
    public void testExecuteWithStatementAndListParameters() {
        final PooledQldbDriver driverSpy = Mockito.spy(pooledQldbDriver);

        Mockito.doReturn(mockQldbSession).when(driverSpy).getSession();
        Mockito.when(mockQldbSession.execute(statement, ionList)).thenReturn(null);

        assertNull(driverSpy.execute(statement, ionList));
        Mockito.verify(mockQldbSession, times(1)).execute(statement, ionList);
    }

    @Test
    public void testExecuteWithStatementAndListParametersAndRetry() {
        final PooledQldbDriver driverSpy = Mockito.spy(pooledQldbDriver);

        Mockito.doReturn(mockQldbSession).when(driverSpy).getSession();
        Mockito.when(mockQldbSession.execute(statement, mockRetryIndicator, ionList)).thenReturn(null);

        assertNull(driverSpy.execute(statement, mockRetryIndicator, ionList));
        Mockito.verify(mockQldbSession, times(1)).execute(statement, mockRetryIndicator, ionList);
    }

    @Test
    public void testExecuteWithStatementAndArgParameters() {
        final PooledQldbDriver driverSpy = Mockito.spy(pooledQldbDriver);

        Mockito.doReturn(mockQldbSession).when(driverSpy).getSession();
        Mockito.when(mockQldbSession.execute(statement, ionList.get(0), ionList.get(1))).thenReturn(null);

        assertNull(driverSpy.execute(statement, ionList.get(0), ionList.get(1)));
        Mockito.verify(mockQldbSession, times(1)).execute(statement, ionList.get(0), ionList.get(1));
    }

    @Test
    public void testExecuteWithStatementAndArgParametersAndRetry() {
        final PooledQldbDriver driverSpy = Mockito.spy(pooledQldbDriver);

        Mockito.doReturn(mockQldbSession).when(driverSpy).getSession();
        Mockito.when(mockQldbSession.execute(statement, mockRetryIndicator, ionList.get(0), ionList.get(1))).thenReturn(null);

        assertNull(driverSpy.execute(statement, mockRetryIndicator, ionList.get(0), ionList.get(1)));
        Mockito.verify(mockQldbSession, times(1)).execute(statement, mockRetryIndicator, ionList.get(0), ionList.get(1));
    }

    @Test
    public void testExecuteWithLambdaWithNoReturnValue() {
        final PooledQldbDriver driverSpy = Mockito.spy(pooledQldbDriver);

        Mockito.doReturn(mockQldbSession).when(driverSpy).getSession();
        driverSpy.execute(Mockito.mock(Executor.class));

        Mockito.verify(mockQldbSession, times(1)).execute(Mockito.any(Executor.class));
    }

    @Test
    public void testExecuteWithLambdaWithNoReturnValueAndRetry() {
        final PooledQldbDriver driverSpy = Mockito.spy(pooledQldbDriver);

        Mockito.doReturn(mockQldbSession).when(driverSpy).getSession();
        driverSpy.execute(Mockito.mock(Executor.class), mockRetryIndicator);

        Mockito.verify(mockQldbSession, times(1)).execute(Mockito.any(Executor.class), Mockito.eq(mockRetryIndicator));
    }

    @Test
    public void testExecuteWithLambdaWithReturnValue() {
        final PooledQldbDriver driverSpy = Mockito.spy(pooledQldbDriver);

        Mockito.doReturn(mockQldbSession).when(driverSpy).getSession();
        Mockito.doReturn(null).when(mockQldbSession).execute(Mockito.mock(Executor.class));

        assertNull(driverSpy.execute(Mockito.mock(Executor.class)));
        Mockito.verify(mockQldbSession, times(1)).execute(Mockito.any(Executor.class));
    }

    @Test
    public void testExecuteWithLambdaWithReturnValueAndRetry() {
        final PooledQldbDriver driverSpy = Mockito.spy(pooledQldbDriver);

        Mockito.doReturn(mockQldbSession).when(driverSpy).getSession();
        Mockito.doReturn(null).when(mockQldbSession).execute(Mockito.mock(Executor.class), mockRetryIndicator);

        assertNull(driverSpy.execute(Mockito.mock(Executor.class), mockRetryIndicator));
        Mockito.verify(mockQldbSession, times(1)).execute(Mockito.any(Executor.class), Mockito.eq(mockRetryIndicator));

    }

    @Test
    public void testGetSession() {
        mockClient.queueResponse(MockResponses.START_SESSION_RESPONSE);
        final PooledQldbDriver driver = PooledQldbDriver.builder()
                .withSessionClientBuilder(mockBuilder)
                .withLedger(LEDGER)
                .withPoolLimit(POOL_LIMIT)
                .build();
        assertEquals(driver.getSession().getLedgerName(), LEDGER);
    }

    @Test
    public void testGetSessionException() {
        final String msg = "msg";
        mockClient.queueResponse(new AmazonClientException(msg));
        final PooledQldbDriver driver = PooledQldbDriver.builder()
                .withSessionClientBuilder(mockBuilder)
                .withLedger(LEDGER)
                .withPoolLimit(POOL_LIMIT)
                .build();
        assertThrows(AmazonClientException.class, driver::getSession, "msg");
    }

    @Test
    public void testGetSessionWithDeadSessionInPool() {
        mockClient.queueResponse(MockResponses.START_SESSION_RESPONSE);
        mockClient.queueResponse(new AmazonClientException("msg"));
        mockClient.queueResponse(MockResponses.START_SESSION_RESPONSE);
        final PooledQldbDriver driver = PooledQldbDriver.builder()
                .withSessionClientBuilder(mockBuilder)
                .withLedger(LEDGER)
                .withPoolLimit(POOL_LIMIT)
                .build();
        driver.getSession().close();
        assertEquals(driver.getSession().getLedgerName(), LEDGER);
    }

    @Test
    public void testGetSessionReachedTimeout() {
        mockClient.queueResponse(MockResponses.START_SESSION_RESPONSE);

        final PooledQldbDriver driver = PooledQldbDriver.builder()
                .withSessionClientBuilder(mockBuilder)
                .withLedger(LEDGER)
                .withPoolLimit(1)
                .withPoolTimeout(0)
                .build();
        driver.getSession();
        assertThrows(QldbClientException.class, driver::getSession);
    }

    @Test
    public void testGetSessionWaiting() {
        mockClient.queueResponse(MockResponses.START_SESSION_RESPONSE);
        mockClient.queueResponse(MockResponses.ABORT_RESPONSE);
        final PooledQldbDriver driver = PooledQldbDriver.builder()
                .withSessionClientBuilder(mockBuilder)
                .withLedger(LEDGER)
                .withPoolLimit(1)
                .build();
        final QldbSession blockingSession = driver.getSession();
        CompletableFuture.runAsync(() -> {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
            blockingSession.close();
        });
        assertEquals(driver.getSession().getLedgerName(), LEDGER);
    }

    @Test
    public void testGetSessionWaitingAbortThrows() {
        mockClient.queueResponse(MockResponses.START_SESSION_RESPONSE);
        mockClient.queueResponse(new AmazonClientException("msg"));
        mockClient.queueResponse(MockResponses.START_SESSION_RESPONSE);
        final PooledQldbDriver driver = PooledQldbDriver.builder()
                .withSessionClientBuilder(mockBuilder)
                .withLedger(LEDGER)
                .withPoolLimit(1)
                .build();
        final QldbSession blockingSession = driver.getSession();
        CompletableFuture.runAsync(() -> {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
            blockingSession.close();
        });
        assertEquals(driver.getSession().getLedgerName(), LEDGER);
    }

    @Test
    public void testGetSessionWaitingAbortThrowsStartThrows() {
        mockClient.queueResponse(MockResponses.START_SESSION_RESPONSE);
        mockClient.queueResponse(new AmazonClientException("msg1"));
        mockClient.queueResponse(new AmazonClientException("msg2"));
        final PooledQldbDriver driver = PooledQldbDriver.builder()
                .withSessionClientBuilder(mockBuilder)
                .withLedger(LEDGER)
                .withPoolLimit(1)
                .build();
        final QldbSession blockingSession = driver.getSession();
        CompletableFuture.runAsync(() -> {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
            blockingSession.close();
        });

        assertThrows(AmazonClientException.class, driver::getSession, "msg2");
    }

    @Test
    public void testReleaseSession() throws IOException {
        mockClient.queueResponse(MockResponses.START_SESSION_RESPONSE);
        mockClient.queueResponse(MockResponses.startTxnResponse("id"));
        mockClient.queueResponse(MockResponses.ABORT_RESPONSE);
        mockClient.queueResponse(MockResponses.executeResponse(ionList));

        final PooledQldbDriver driver = PooledQldbDriver.builder()
                .withSessionClientBuilder(mockBuilder)
                .withLedger(LEDGER)
                .withPoolLimit(POOL_LIMIT)
                .build();
        final QldbSession session = driver.getSession();
        final Transaction txnFromPooledSession = session.startTransaction();
        session.close();

        // This will throw an exception if it doesn't reuse the old session since only
        // one start session response is queued.
        driver.getSession();

        // This should not throw an exception since the transaction is still alive when the session is returned.
        txnFromPooledSession.execute("foo");
    }
}
