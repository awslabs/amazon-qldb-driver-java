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

import com.amazon.ion.IonSystem;
import com.amazon.ion.IonValue;
import com.amazon.ion.system.IonSystemBuilder;
import com.amazonaws.ClientConfigurationFactory;
import com.amazonaws.services.qldbsession.AmazonQLDBSessionClientBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

public class TestQldbDriver {
    private static final AmazonQLDBSessionClientBuilder CLIENT_BUILDER = AmazonQLDBSessionClientBuilder.standard()
            .withRegion("us-east-1");
    private static final String LEDGER = "ledger";
    private static final int READ_AHEAD = 2;
    private static final int RETRY_LIMIT = 3;
    private IonSystem system;
    private List<IonValue> ionList;
    private String statement;
    private QldbDriver driver;


    private final MockQldbSessionClient mockClient = new MockQldbSessionClient();

    @Mock
    private AmazonQLDBSessionClientBuilder mockBuilder;

    @Mock
    private RetryIndicator mockRetryIndicator;

    @Mock
    private QldbSession mockQldbSession;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void init() {
        MockitoAnnotations.initMocks(this);

        system = IonSystemBuilder.standard().build();
        ionList  = new ArrayList<>(2);
        ionList.add(system.newString("a"));
        ionList.add(system.newString("b"));
        statement = "select * from test";

        Mockito.when(mockBuilder.build()).thenReturn(mockClient);
        Mockito.when(mockBuilder.getClientConfiguration()).thenReturn(new ClientConfigurationFactory().getConfig());

        driver = QldbDriver.builder()
                .withLedger(LEDGER)
                .withRetryLimit(RETRY_LIMIT)
                .withSessionClientBuilder(mockBuilder)
                .build();
    }

    // This is a test for QldbDriverBuilder
    @Test
    public void testBuildWithBlankLedger() {
        thrown.expect(IllegalArgumentException.class);

        QldbDriver.builder()
                .withSessionClientBuilder(CLIENT_BUILDER)
                .build();
    }

    // This is a test for QldbDriverBuilder
    @Test
    public void testBuildWithNullClientBuilder() {
        thrown.expect(IllegalArgumentException.class);

        QldbDriver.builder()
                .withLedger(LEDGER)
                .build();
    }

    // This is a test for QldbDriverBuilder
    @Test
    public void testBuildWithOccConflictRetryLimitNegativeInt() {
        thrown.expect(IllegalArgumentException.class);

        QldbDriver.builder()
                .withRetryLimit(-1)
                .build();
    }

    // This is a test for QldbDriverBuilder
    @Test
    public void testBuildWithOccConflictRetryLimitZero() {
        final QldbDriver.QldbDriverBuilder builder = QldbDriver.builder()
                .withRetryLimit(0)
                .withSessionClientBuilder(CLIENT_BUILDER)
                .withLedger(LEDGER);
        Assert.assertNotNull(builder.build());
    }

    // This is a test for QldbDriverBuilder
    @Test
    public void testBuildWithOccConflictRetryLimitPositive() {
        final QldbDriver.QldbDriverBuilder builder = QldbDriver.builder()
                .withRetryLimit(1)
                .withSessionClientBuilder(CLIENT_BUILDER)
                .withLedger(LEDGER);
        Assert.assertNotNull(builder.build());
    }

    // This is a test for QldbDriverBuilder
    @Test
    public void testBuildWithNullIonSystem() {
        thrown.expect(IllegalArgumentException.class);

        QldbDriver.builder()
                .withIonSystem(null)
                .build();
    }

    // This is a test for QldbDriverBuilder
    @Test
    public void testBuildWithIonSystem() {
        final QldbDriver.QldbDriverBuilder builder = QldbDriver.builder()
                .withIonSystem(IonSystemBuilder.standard().build())
                .withSessionClientBuilder(CLIENT_BUILDER)
                .withLedger(LEDGER);
        Assert.assertNotNull(builder.build());
    }

    // This is a test for QldbDriverBuilder
    @Test
    public void testBuildWithReadAheadNegativeInt() {
        thrown.expect(IllegalArgumentException.class);

        QldbDriver.builder()
                .withReadAhead(-1)
                .build();
    }

    // This is a test for QldbDriverBuilder
    @Test
    public void testBuildWithReadAheadPositive() {
        final QldbDriver.QldbDriverBuilder builder = QldbDriver.builder()
                .withReadAhead(READ_AHEAD)
                .withSessionClientBuilder(CLIENT_BUILDER)
                .withLedger(LEDGER);
        Assert.assertNotNull(builder.build());
    }

    // This is a test for QldbDriverBuilder
    @Test
    public void testBuildWithReadAheadPositiveNullExecutor() {
        thrown.expect(IllegalArgumentException.class);

        QldbDriver.builder()
                .withReadAhead(1, null)
                .build();
    }

    // This is a test for QldbDriverBuilder
    @Test
    public void testBuildWithReadAheadPositiveAndExecutor() {
        final QldbDriver.QldbDriverBuilder builder = QldbDriver.builder()
                .withReadAhead(1, Mockito.mock(ExecutorService.class))
                .withSessionClientBuilder(CLIENT_BUILDER)
                .withLedger(LEDGER);
        Assert.assertNotNull(builder.build());
    }

    // This is a test for QldbDriverBuilder
    @Test
    public void testBuildWithReadAheadLessThanTwo() {
        thrown.expect(IllegalArgumentException.class);

        final QldbDriver.QldbDriverBuilder builder = QldbDriver.builder()
                .withReadAhead(1)
                .withSessionClientBuilder(CLIENT_BUILDER)
                .withLedger(LEDGER);
        Assert.assertNotNull(builder.build());
    }

    // This is a test for QldbDriverBuilder
    @Test
    public void testBuildWithDefaultClientWithOccConflictRetryLimitNegativeInt() {
        thrown.expect(IllegalArgumentException.class);

        QldbDriver.builder()
                .withRetryLimit(-1)
                .build();
    }

    @Test
    public void testGetSession() {
        mockClient.queueResponse(MockResponses.START_SESSION_RESPONSE);
        Assert.assertEquals(driver.getSession().getLedgerName(), LEDGER);
    }

    @Test
    public void testGetSessionWhenClosed() {
        mockClient.queueResponse(MockResponses.START_SESSION_RESPONSE);
        mockClient.queueResponse(MockResponses.endSessionResponse());

        thrown.expect(IllegalStateException.class);

        try {
            driver.getSession().close();
            driver.close();
        } catch (IllegalStateException ise) {
            Assert.fail("IllegalStateException should not occur here.");
        }
        driver.getSession();
    }
}
