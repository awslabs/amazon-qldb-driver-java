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

package software.amazon.qldb.integrationtests;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.amazonaws.services.qldbsession.model.BadRequestException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import software.amazon.qldb.PooledQldbDriver;
import software.amazon.qldb.QldbSession;
import software.amazon.qldb.exceptions.QldbClientException;
import software.amazon.qldb.integrationtests.utils.Constants;
import software.amazon.qldb.integrationtests.utils.IntegrationTestBase;

public class SessionManagementIntegTest {
    private static IntegrationTestBase integrationTestBase;

    @BeforeAll
    public static void setup() throws InterruptedException {
        integrationTestBase = new IntegrationTestBase(Constants.LEDGER_NAME, System.getProperty("region"));

        integrationTestBase.runCreateLedger();
    }

    @AfterAll
    public static void classCleanup() throws InterruptedException {
        integrationTestBase.deleteLedger();
    }

    @Test
    public void connect_LedgerDoesNotExist_ThrowsBadRequestException() {
        PooledQldbDriver driver = integrationTestBase.createQldbDriver(
            Constants.NON_EXISTENT_LEDGER_NAME, 0, 0, Constants.DEFAULT);

        assertThrows(BadRequestException.class, () -> driver.getSession().getTableNames());

        driver.close();
    }

    @Test
    public void getSession_PoolDoesNotHaveSessionAndHasNotHitLimit_DoesNotThrowException() {
        // Start a pooled driver with default pool limit so it doesn't have sessions in the pool
        // and has not hit the limit.
        PooledQldbDriver driver = integrationTestBase.createQldbDriver(Constants.DEFAULT, Constants.DEFAULT, Constants.DEFAULT);

        assertDoesNotThrow(() -> driver.getSession().getTableNames());

        driver.close();
    }

    @Test
    public void getSession_PoolHasSessionAndHasNotHitLimit_DoesNotThrowException() {
        // Start a pooled driver with default pool limit so it doesn't have sessions in the pool
        // and has not hit the limit.
        PooledQldbDriver driver = integrationTestBase.createQldbDriver(Constants.DEFAULT, Constants.DEFAULT, Constants.DEFAULT);

        assertDoesNotThrow(
            () -> {
                // Call the first getTableNames() to start a session and put into pool.
                driver.getSession().getTableNames();

                // Call the second getTableNames() to use session from pool and is expected to execute successfully.
                driver.getSession().getTableNames();
            });

        driver.close();
    }

    @Test
    public void getSession_PoolDoesNotHaveSessionAndHasHitLimit_ThrowsQldbClientException() throws Throwable {
        PooledQldbDriver driver = integrationTestBase.createQldbDriver(1, 1, Constants.DEFAULT);

        // With the poolTimeout to just 1 ms, only one thread should go through.
        // The other two threads will try to acquire the session, but because it can wait for only 1ms,
        // they will error out.
        final int numThreads = 3;
        AtomicReference<Throwable> childThreadException = new AtomicReference<>(null);

        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < numThreads; i++) {
            Thread thread = new Thread(() -> driver.getSession().getTableNames());

            // If thread throws an exception, store the exception so the main thread will have access to it
            Thread.UncaughtExceptionHandler handler = (th, ex) -> childThreadException.set(ex);
            thread.setUncaughtExceptionHandler(handler);

            threads.add(thread);
        }

        for (Thread thread : threads) {
            thread.start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        assertTrue(childThreadException.get() instanceof QldbClientException);

        driver.close();
    }

    @Test
    public void getSession_PoolDoesNotHaveSessionAndHasHitLimitAndSessionReturnedToPool_DoesNotThrowException()
            throws InterruptedException {
        // Start a pooled driver with pool limit of 1 and default timeout of 30 seconds.
        PooledQldbDriver driver = integrationTestBase.createQldbDriver(1, Constants.DEFAULT, Constants.DEFAULT);

        // Start two threads to execute getTableNames() concurrently which will hit the session pool limit but
        // will succeed because session is returned to pool within the timeout.
        final int numThreads = 2;
        AtomicReference<Throwable> childThreadException = new AtomicReference<>(null);

        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < numThreads; i++) {
            Thread thread = new Thread(
                () -> {
                    QldbSession session = driver.getSession();
                    session.getTableNames();
                    session.close();
                });

            // If thread throws an exception, store the exception so the main thread will have access to it
            Thread.UncaughtExceptionHandler handler = (th, ex) -> childThreadException.set(ex);
            thread.setUncaughtExceptionHandler(handler);

            threads.add(thread);
        }

        for (Thread thread : threads) {
            thread.start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        assertNull(childThreadException.get());

        driver.close();
    }

    @Test
    public void getSession_DriverIsClosed_ThrowsIllegalStateException() {
        PooledQldbDriver driver = integrationTestBase.createQldbDriver(1, 3000, Constants.DEFAULT);

        driver.close();

        assertThrows(IllegalStateException.class, () -> driver.getSession().getTableNames());
    }
}
