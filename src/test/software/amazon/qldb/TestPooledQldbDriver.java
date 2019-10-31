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

import java.util.concurrent.CompletableFuture;

import com.amazonaws.AmazonClientException;
import com.amazonaws.ClientConfigurationFactory;
import com.amazonaws.services.qldbsession.AmazonQLDBSessionClientBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import software.amazon.qldb.exceptions.QldbClientException;

public class TestPooledQldbDriver {
    private static final String LEDGER = "ledger";
    private static final int POOL_LIMIT = 2;

    private final MockQldbSessionClient mockClient = new MockQldbSessionClient();

    @Mock
    private AmazonQLDBSessionClientBuilder mockBuilder;

    @Before
    public void init() {
        MockitoAnnotations.initMocks(this);

        Mockito.when(mockBuilder.build()).thenReturn(mockClient);
        Mockito.when(mockBuilder.getClientConfiguration()).thenReturn(new ClientConfigurationFactory().getConfig());
    }

    // This is a test for PooledQldbDriverBuilder
    @Test
    public void testBuildWithPoolLimit() {
        PooledQldbDriver.builder()
                .withSessionClientBuilder(mockBuilder)
                .withLedger(LEDGER)
                .withPoolLimit(POOL_LIMIT)
                .build();
    }

    // This is a test for PooledQldbDriverBuilder
    @Test (expected = QldbClientException.class)
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
        driver.getSession();
    }

    // This is a test for PooledQldbDriverBuilder
    @Test (expected = IllegalArgumentException.class)
    public void testBuildWithPoolLimitNegative() {
        PooledQldbDriver.builder()
                .withSessionClientBuilder(mockBuilder)
                .withLedger(LEDGER)
                .withPoolLimit(-1)
                .build();
    }

    // This is a test for PooledQldbDriverBuilder
    @Test (expected = IllegalArgumentException.class)
    public void testBuildWithPoolLimitGreaterThanConfigLimit() {
        // Default for the builder is 50.
        PooledQldbDriver.builder()
                .withSessionClientBuilder(mockBuilder)
                .withLedger(LEDGER)
                .withPoolLimit(51)
                .build();
    }

    @Test (expected = IllegalStateException.class)
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
            Assert.fail("IllegalStateException should not occur here.");
        }
        driver.getSession();
    }

    @Test
    public void testGetSession() {
        mockClient.queueResponse(MockResponses.START_SESSION_RESPONSE);
        final PooledQldbDriver driver = PooledQldbDriver.builder()
                .withSessionClientBuilder(mockBuilder)
                .withLedger(LEDGER)
                .withPoolLimit(POOL_LIMIT)
                .build();
        Assert.assertEquals(driver.getSession().getLedgerName(), LEDGER);
    }

    @Test (expected = AmazonClientException.class)
    public void testGetSessionException() {
        final String msg = "msg";
        mockClient.queueResponse(new AmazonClientException(msg));
        final PooledQldbDriver driver = PooledQldbDriver.builder()
                .withSessionClientBuilder(mockBuilder)
                .withLedger(LEDGER)
                .withPoolLimit(POOL_LIMIT)
                .build();
        try {
            QldbSession session = driver.getSession();
        } catch (AmazonClientException ace) {
            Assert.assertEquals(ace.getMessage(), msg);
            throw ace;
        }
    }

    @Test
    public void testGetSessionWithDeadSessionInPool() {
        mockClient.queueResponse(MockResponses.START_SESSION_RESPONSE);
        mockClient.queueResponse(new AmazonClientException("msg"));
        mockClient.queueResponse(MockResponses.START_SESSION_RESPONSE);
        PooledQldbDriver driver = PooledQldbDriver.builder()
                .withSessionClientBuilder(mockBuilder)
                .withLedger(LEDGER)
                .withPoolLimit(POOL_LIMIT)
                .build();
        driver.getSession().close();
        Assert.assertEquals(driver.getSession().getLedgerName(), LEDGER);
    }

    @Test (expected = QldbClientException.class)
    public void testGetSessionReachedTimeout() {
        mockClient.queueResponse(MockResponses.START_SESSION_RESPONSE);
        PooledQldbDriver driver = PooledQldbDriver.builder()
                .withSessionClientBuilder(mockBuilder)
                .withLedger(LEDGER)
                .withPoolLimit(1)
                .withPoolTimeout(0)
                .build();
        driver.getSession();
        driver.getSession();
    }

    @Test
    public void testGetSessionWaiting() {
        mockClient.queueResponse(MockResponses.START_SESSION_RESPONSE);
        mockClient.queueResponse(MockResponses.ABORT_RESPONSE);
        PooledQldbDriver driver = PooledQldbDriver.builder()
                .withSessionClientBuilder(mockBuilder)
                .withLedger(LEDGER)
                .withPoolLimit(1)
                .build();
        QldbSession blockingSession = driver.getSession();
        CompletableFuture.runAsync(() -> {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
            blockingSession.close();
        });
        Assert.assertEquals(driver.getSession().getLedgerName(), LEDGER);
    }

    @Test
    public void testGetSessionWaitingAbortThrows() {
        mockClient.queueResponse(MockResponses.START_SESSION_RESPONSE);
        mockClient.queueResponse(new AmazonClientException("msg"));
        mockClient.queueResponse(MockResponses.START_SESSION_RESPONSE);
        PooledQldbDriver driver = PooledQldbDriver.builder()
                .withSessionClientBuilder(mockBuilder)
                .withLedger(LEDGER)
                .withPoolLimit(1)
                .build();
        QldbSession blockingSession = driver.getSession();
        CompletableFuture.runAsync(() -> {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
            blockingSession.close();
        });
        Assert.assertEquals(driver.getSession().getLedgerName(), LEDGER);
    }

    @Test (expected = AmazonClientException.class)
    public void testGetSessionWaitingAbortThrowsStartThrows() {
        mockClient.queueResponse(MockResponses.START_SESSION_RESPONSE);
        mockClient.queueResponse(new AmazonClientException("msg1"));
        mockClient.queueResponse(new AmazonClientException("msg2"));
        PooledQldbDriver driver = PooledQldbDriver.builder()
                .withSessionClientBuilder(mockBuilder)
                .withLedger(LEDGER)
                .withPoolLimit(1)
                .build();
        QldbSession blockingSession = driver.getSession();
        CompletableFuture.runAsync(() -> {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
            blockingSession.close();
        });
        try {
            driver.getSession();
        } catch (AmazonClientException ace) {
            Assert.assertEquals(ace.getMessage(), "msg2");
            throw ace;
        }
    }

    @Test
    public void testReleaseSession() {
        mockClient.queueResponse(MockResponses.START_SESSION_RESPONSE);
        mockClient.queueResponse(MockResponses.ABORT_RESPONSE);
        PooledQldbDriver driver = PooledQldbDriver.builder()
                .withSessionClientBuilder(mockBuilder)
                .withLedger(LEDGER)
                .withPoolLimit(POOL_LIMIT)
                .build();
        final QldbSession session = driver.getSession();
        session.close();

        // This will throw an exception if it doesn't reuse the old session since only one start session response is queued.
        driver.getSession();
    }
}
