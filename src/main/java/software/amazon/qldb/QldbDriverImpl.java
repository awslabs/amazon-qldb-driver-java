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

import com.amazon.ion.IonSystem;
import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.annotations.ThreadSafe;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.qldbsession.QldbSessionClient;
import software.amazon.awssdk.utils.Validate;
import software.amazon.qldb.exceptions.Errors;
import software.amazon.qldb.exceptions.ExecuteException;
import software.amazon.qldb.exceptions.QldbDriverException;

/**
 * Implementation of the QldbDriver.
 *
 * {@link QldbDriver} is the main entry point to execute transactions with a QLDB Ledger.
 *
 * <p>The implementation executes a transaction when a lambda function is passed to any of
 * its execute methods.
 *
 * <p>This object is thread-safe.</p>
 */
@ThreadSafe
class QldbDriverImpl implements QldbDriver {
    static final String TABLE_NAME_QUERY =
        "SELECT VALUE name FROM information_schema.user_tables WHERE status = 'ACTIVE'";
    private static final Logger logger = LoggerFactory.getLogger(QldbDriver.class);
    private static final long DEFAULT_TIMEOUT_MS = 1;

    private final String ledgerName;

    private final Semaphore poolPermits;
    private final BlockingQueue<QldbSession> pool;

    private final int readAhead;
    private final ExecutorService executorService;
    private final QldbSessionClient amazonQldbSession;
    private final RetryPolicy retryPolicy;
    private final IonSystem ionSystem;
    private final AtomicBoolean isClosed;


    /**
     * Constructor for the for the pool driver. To create an instance of the QldbDriver
     * use the {@link QldbDriverBuilder}.
     *
     * @param ledgerName
     *                  The ledger to create sessions to.
     * @param qldbSessionClient
     *                  The low-level session used for communication with QLDB.
     * @param retryPolicy
     *                  The retry policy that specifes how many times it should be retried and how long to delay the next retry
     *                  upon encountering a non-fatal error.
     * @param readAhead
     *                  The number of read-ahead buffers for each open result set created from sessions from this
     *                  driver.
     * @param maxConcurrentTransactions
     *                  Specifies how many concurrent transactions can be executed.
     * @param ionSystem
     *                  The {@link IonSystem} sessions created by this driver will use.
     * @param executorService
     *                  The executor to be used by the retrieval thread if read-ahead is enabled.
     */
    protected QldbDriverImpl(String ledgerName,
                             QldbSessionClient qldbSessionClient,
                             RetryPolicy retryPolicy,
                             int readAhead,
                             int maxConcurrentTransactions,
                             IonSystem ionSystem,
                             ExecutorService executorService) {
        this.ledgerName = ledgerName;
        this.amazonQldbSession = qldbSessionClient;
        this.retryPolicy = retryPolicy;
        this.ionSystem = ionSystem;
        this.isClosed = new AtomicBoolean(false);
        this.readAhead = readAhead;
        this.executorService = executorService;

        this.poolPermits = new Semaphore(maxConcurrentTransactions, true);
        this.pool = new LinkedBlockingQueue<>();
    }

    @Override
    public void close() {
        if (!isClosed.getAndSet(true)) {
            for (QldbSession curSession = pool.poll(); curSession != null; curSession = pool.poll()) {
                curSession.close();
            }
        }
    }

    @Override
    public void execute(ExecutorNoReturn executor) {
        execute(executor, retryPolicy);
    }

    @Override
    public void execute(ExecutorNoReturn executor, RetryPolicy retryPolicy) {
        execute(txn -> {
            executor.execute(txn);
            return Boolean.TRUE;
        }, retryPolicy);
    }

    @Override
    public <T> T execute(Executor<T> executor) {
        return execute(executor, retryPolicy);
    }

    @Override
    public <T> T execute(Executor<T> executor, RetryPolicy retryPolicy) {
        Validate.paramNotNull(executor, "executor");
        Validate.notNull(retryPolicy, "retryPolicy");
        if (isClosed.get()) {
            logger.error(Errors.DRIVER_CLOSED.get());
            throw QldbDriverException.create(Errors.DRIVER_CLOSED.get());
        }

        boolean replaceDeadSession = false;
        int retryAttempt = 0;
        while (true) {
            QldbSession session = null;
            try {
                if (replaceDeadSession) {
                    session = this.createNewSession();
                } else {
                    session = this.getSession();
                }
                T returnedValue = session.execute(executor);
                this.releaseSession(session);
                return returnedValue;
            } catch (ExecuteException ee) {
                retryAttempt++;
                // If initial session is invalid, always retry once with a new session.
                if (ee.isRetryable() && ee.isInvalidSessionException() && retryAttempt == 1) {
                    logger.debug("Initial session received from pool invalid. Retrying...");
                    replaceDeadSession = true;
                    continue;
                }
                // Do not retry.
                if (!ee.isRetryable() || retryAttempt > retryPolicy.maxRetries()) {
                    if (ee.isSessionAlive() && session != null) {
                        this.releaseSession(session);
                    } else {
                        this.poolPermits.release();
                    }
                    throw ee.getCause();
                }
                // Retry.
                logger.info("A recoverable error has occurred. Attempting retry #{}.", retryAttempt);
                logger.debug("Errored Transaction ID: {}. Error cause: ", ee.getTransactionId(), ee.getCause());
                replaceDeadSession = !ee.isSessionAlive();
                if (replaceDeadSession) {
                    logger.debug("Replacing invalid session...");
                } else {
                    logger.debug("Retrying with a different session...");
                    this.releaseSession(session);
                }

                try {
                    // This can be safely casted since only SdkExceptions or child Exceptions are deemed retryable.
                    final SdkException se = (SdkException) ee.getCause();
                    final RetryPolicyContext context = new RetryPolicyContext(se, retryAttempt, ee.getTransactionId());
                    retrySleep(context, retryPolicy);
                } catch (Exception e) {
                    if (replaceDeadSession) {
                        // Safeguard against semaphore leak if parameter actions throw exceptions.
                        this.poolPermits.release();
                    }
                    throw e;
                }
            }
        }
    }

    @Override
    public Iterable<String> getTableNames() {
        final Result result = execute(txn -> {
            return txn.execute(TABLE_NAME_QUERY);
        }, retryPolicy);
        return new TableNameIterable(result);
    }

    private QldbSession getSession() {
        logger.debug("Getting session. There are {} free sessions; currently available permits is: {}.",
                pool.size(), poolPermits.availablePermits());

        try {
            if (poolPermits.tryAcquire(DEFAULT_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                QldbSession session = pool.poll();
                if (session == null) {
                    session = createNewSession();
                    logger.debug("Creating new pooled session. Session ID: {}.", session.getSessionId());
                }
                return session;
            } else {
                throw QldbDriverException.create(String.format(Errors.NO_SESSION_AVAILABLE.get(), DEFAULT_TIMEOUT_MS));
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw QldbDriverException.create(Errors.GET_SESSION_INTERRUPTED.get());
        }
    }

    private QldbSession createNewSession() {
        try {
            final Session session = Session.startSession(ledgerName, amazonQldbSession);
            return new QldbSession(session, readAhead, ionSystem, executorService);
        } catch (SdkException ase) {
            throw new ExecuteException(ase, true, false, true, "None");
        }
    }

    private void releaseSession(QldbSession session) {
        pool.add(session);
        poolPermits.release();
        logger.debug("Session returned to pool; pool size is now: {}.", pool.size());
    }

    private void retrySleep(RetryPolicyContext context, RetryPolicy retryPolicy) {
        try {
            Duration backoffDelay =
                    retryPolicy.backoffStrategy().calculateDelay(context);
            if (backoffDelay == null || backoffDelay.isNegative()) {
                backoffDelay = Duration.ofMillis(0);
            }

            TimeUnit.MILLISECONDS.sleep(backoffDelay.toMillis());
        } catch (InterruptedException e) {
            // Reset the interruption flag.
            Thread.currentThread().interrupt();
        }
    }
}
