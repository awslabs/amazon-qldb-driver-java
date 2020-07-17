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
import static org.junit.jupiter.api.Assertions.fail;

import com.amazon.ion.system.IonSystemBuilder;
import com.amazonaws.ClientConfigurationFactory;
import com.amazonaws.services.qldbsession.AmazonQLDBSessionClientBuilder;
import java.util.concurrent.ExecutorService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

public class TestQldbDriver {
    private static final AmazonQLDBSessionClientBuilder CLIENT_BUILDER = AmazonQLDBSessionClientBuilder.standard()
            .withRegion("us-east-1");
    private static final String LEDGER = "ledger";
    private static final int READ_AHEAD = 2;
    private static final int RETRY_LIMIT = 3;
    private QldbDriver driver;

    private final MockQldbSessionClient mockClient = new MockQldbSessionClient();

    @Mock
    private AmazonQLDBSessionClientBuilder mockBuilder;

    @BeforeEach
    public void init() {
        MockitoAnnotations.initMocks(this);

        Mockito.when(mockBuilder.build()).thenReturn(mockClient);
        Mockito.when(mockBuilder.getClientConfiguration()).thenReturn(new ClientConfigurationFactory().getConfig());

        driver = QldbDriver.builder()
                .withLedger(LEDGER)
                .withRetryLimit(RETRY_LIMIT)
                .withSessionClientBuilder(mockBuilder)
                .build();
    }

    // This is a test for QldbDriverBuilder.
    @Test
    public void testBuildWithBlankLedger() {
        assertThrows(IllegalArgumentException.class,
            () -> QldbDriver.builder()
                    .withSessionClientBuilder(CLIENT_BUILDER)
                    .build());
    }

    // This is a test for QldbDriverBuilder.
    @Test
    public void testBuildWithNullClientBuilder() {
        assertThrows(IllegalArgumentException.class,
            () -> QldbDriver.builder()
                    .withLedger(LEDGER)
                    .build());
    }

    // This is a test for QldbDriverBuilder.
    @Test
    public void testBuildWithOccConflictRetryLimitNegativeInt() {
        assertThrows(IllegalArgumentException.class,
            () -> QldbDriver.builder()
                    .withRetryLimit(-1)
                    .build());
    }

    // This is a test for QldbDriverBuilder.
    @Test
    public void testBuildWithOccConflictRetryLimitZero() {
        final QldbDriver.QldbDriverBuilder builder = QldbDriver.builder()
                .withRetryLimit(0)
                .withSessionClientBuilder(CLIENT_BUILDER)
                .withLedger(LEDGER);
        assertNotNull(builder.build());
    }

    // This is a test for QldbDriverBuilder.
    @Test
    public void testBuildWithOccConflictRetryLimitPositive() {
        final QldbDriver.QldbDriverBuilder builder = QldbDriver.builder()
                .withRetryLimit(1)
                .withSessionClientBuilder(CLIENT_BUILDER)
                .withLedger(LEDGER);
        assertNotNull(builder.build());
    }

    // This is a test for QldbDriverBuilder.
    @Test
    public void testBuildWithNullIonSystem() {
        assertThrows(IllegalArgumentException.class,
            () -> QldbDriver.builder()
                    .withIonSystem(null)
                    .build());
    }

    // This is a test for QldbDriverBuilder.
    @Test
    public void testBuildWithIonSystem() {
        final QldbDriver.QldbDriverBuilder builder = QldbDriver.builder()
                .withIonSystem(IonSystemBuilder.standard().build())
                .withSessionClientBuilder(CLIENT_BUILDER)
                .withLedger(LEDGER);
        assertNotNull(builder.build());
    }

    // This is a test for QldbDriverBuilder.
    @Test
    public void testBuildWithReadAheadNegativeInt() {
        assertThrows(IllegalArgumentException.class,
            () -> QldbDriver.builder()
                    .withReadAhead(-1)
                    .build());
    }

    // This is a test for QldbDriverBuilder.
    @Test
    public void testBuildWithReadAheadPositive() {
        final QldbDriver.QldbDriverBuilder builder = QldbDriver.builder()
                .withReadAhead(READ_AHEAD)
                .withSessionClientBuilder(CLIENT_BUILDER)
                .withLedger(LEDGER);
        assertNotNull(builder.build());
    }

    // This is a test for QldbDriverBuilder.
    @Test
    public void testBuildWithReadAheadPositiveNullExecutor() {
        assertThrows(IllegalArgumentException.class,
            () -> QldbDriver.builder()
                    .withReadAhead(1, null)
                    .build());
    }

    // This is a test for QldbDriverBuilder.
    @Test
    public void testBuildWithReadAheadPositiveAndExecutor() {
        final QldbDriver.QldbDriverBuilder builder = QldbDriver.builder()
                .withReadAhead(1, Mockito.mock(ExecutorService.class))
                .withSessionClientBuilder(CLIENT_BUILDER)
                .withLedger(LEDGER);
        assertNotNull(builder.build());
    }

    // This is a test for QldbDriverBuilder.
    @Test
    public void testBuildWithReadAheadLessThanTwo() {
        assertThrows(IllegalArgumentException.class,
            () -> {
                final QldbDriver.QldbDriverBuilder builder = QldbDriver.builder()
                        .withReadAhead(1)
                        .withSessionClientBuilder(CLIENT_BUILDER)
                        .withLedger(LEDGER);
                assertNotNull(builder.build());
            });
    }

    // This is a test for QldbDriverBuilder.
    @Test
    public void testBuildWithDefaultClientWithOccConflictRetryLimitNegativeInt() {
        assertThrows(IllegalArgumentException.class,
            () -> QldbDriver.builder()
                    .withRetryLimit(-1)
                    .build());
    }

    @Test
    public void testGetSession() {
        mockClient.queueResponse(MockResponses.START_SESSION_RESPONSE);
        assertEquals(driver.getSession().getLedgerName(), LEDGER);
    }

    @Test
    public void testGetSessionWhenClosed() {
        mockClient.queueResponse(MockResponses.START_SESSION_RESPONSE);
        mockClient.queueResponse(MockResponses.endSessionResponse());

        try {
            driver.getSession().close();
            driver.close();
        } catch (IllegalStateException ise) {
            fail("IllegalStateException should not occur here.");
        }

        assertThrows(IllegalStateException.class,
            () -> driver.getSession());
    }
}
