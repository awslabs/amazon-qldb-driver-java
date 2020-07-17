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
import com.amazonaws.annotation.ThreadSafe;
import com.amazonaws.services.qldbsession.AmazonQLDBSession;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.qldb.exceptions.AbortException;
import software.amazon.qldb.exceptions.Errors;
import software.amazon.qldb.exceptions.QldbClientException;

/**
 * <p>Represents a factory for accessing pooled sessions to a specific ledger within QLDB. This class or
 * {@link QldbDriver} should be the main entry points to any interaction with QLDB. {@link #getSession()} will create a
 * {@link PooledQldbSession} to the specified ledger within QLDB as a communication channel. Any acquired sessions must
 * be cleaned up with {@link PooledQldbSession#close()} when they are no longer needed in order to return the session to
 * the pool. If this is not done, this driver may become unusable if the pool limit is exceeded.</p>
 *
 * <p>This factory pools sessions and attempts to return unused but available sessions when getting new sessions. The
 * advantage to using this over the non-pooling driver is that the underlying connection that sessions use to
 * communicate with QLDB can be recycled, minimizing resource usage by preventing unnecessary connections and reducing
 * latency by not making unnecessary requests to start new connections and end reusable, existing, ones.</p>
 *
 * <p>The pool does not remove stale sessions until a new session is retrieved. The default pool size is the maximum
 * amount of connections the session client allows set in the {@link com.amazonaws.ClientConfiguration} of the
 * {@link com.amazonaws.services.qldbsession.AmazonQLDBSessionClientBuilder}. {@link #close()} should be called when
 * this factory is no longer needed in order to clean up resources, ending all sessions in the pool.</p>
 *
 * <p>This object is thread-safe.</p>
 */
@ThreadSafe
public class PooledQldbDriver extends BaseSyncQldbDriver implements RetriableExecutable {
    private static final Logger logger = LoggerFactory.getLogger(PooledQldbDriver.class);
    private static final long DEFAULT_TIMEOUT_MS = 30000;

    private final long timeout;
    private final Semaphore poolPermits;
    private final BlockingQueue<QldbSessionImpl> pool;

    protected PooledQldbDriver(String ledgerName, AmazonQLDBSession amazonQldbSession, int retryLimit, int readAhead,
                               int poolLimit, long timeout, IonSystem ionSystem, ExecutorService executorService) {
        super(ledgerName, amazonQldbSession, retryLimit, readAhead, ionSystem, executorService);

        this.timeout = timeout;
        this.poolPermits = new Semaphore(poolLimit, true);
        this.pool = new LinkedBlockingQueue<>();
    }

    /**
     * Retrieve a builder object for creating a {@link PooledQldbDriver}.
     *
     * @return The builder object for creating a {@link PooledQldbDriver}.
     */
    public static PooledQldbDriverBuilder builder() {
        return new PooledQldbDriverBuilder();
    }

    @Override
    public void close() {
        if (!isClosed.getAndSet(true)) {
            for (QldbSessionImpl curSession = pool.poll(); curSession != null; curSession = pool.poll()) {
                curSession.close();
            }
        }
    }

    /**
     * Execute the statement against QLDB and retrieve the result.
     *
     * @param statement
     *              The PartiQL statement to be executed against QLDB.
     *
     * @return The result of executing the statement.
     * @throws IllegalStateException if this driver has been closed.
     * @throws com.amazonaws.AmazonClientException if there is an error executing against QLDB.
     */
    @Override
    public Result execute(String statement) {
        try (QldbSession qldbSession = getSession()) {
            return qldbSession.execute(statement);
        }
    }

    /**
     * Execute the statement against QLDB and retrieve the result.
     *
     * @param statement
     *              The PartiQL statement to be executed against QLDB.
     * @param retryIndicator
     *              A lambda that which is invoked when the Executor lambda is about to be retried due to a retriable
     *              error. Can be null if not applicable.
     *
     * @return The result of executing the statement.
     * @throws IllegalStateException if this driver has been closed.
     * @throws com.amazonaws.AmazonClientException if there is an error executing against QLDB.
     */
    @Override
    public Result execute(String statement, RetryIndicator retryIndicator) {
        try (QldbSession qldbSession = getSession()) {
            return qldbSession.execute(statement, retryIndicator);
        }
    }

    /**
     * Execute the statement using the specified parameters against QLDB and retrieve the result.
     *
     * @param statement
     *              The PartiQL statement to be executed against QLDB.
     * @param parameters
     *              The parameters to be used with the PartiQL statement, for each ? placeholder in the statement.
     *
     * @return The result of executing the statement.
     * @throws IllegalStateException if this driver has been closed.
     * @throws com.amazonaws.AmazonClientException if there is an error executing against QLDB.
     */
    @Override
    public Result execute(String statement, List<IonValue> parameters) {
        try (QldbSession qldbSession = getSession()) {
            return qldbSession.execute(statement, parameters);
        }
    }

    /**
     * Execute the statement using the specified parameters against QLDB and retrieve the result.
     *
     * @param statement
     *              The PartiQL statement to be executed against QLDB.
     * @param retryIndicator
     *              A lambda that which is invoked when the Executor lambda is about to be retried due to a retriable
     *              error. Can be null if not applicable.
     * @param parameters
     *              The parameters to be used with the PartiQL statement, for each ? placeholder in the statement.
     *
     * @return The result of executing the statement.
     * @throws IllegalStateException if this driver has been closed.
     * @throws com.amazonaws.AmazonClientException if there is an error executing against QLDB.
     */
    @Override
    public Result execute(String statement, RetryIndicator retryIndicator, List<IonValue> parameters) {
        try (QldbSession qldbSession = getSession()) {
            return qldbSession.execute(statement, retryIndicator, parameters);
        }
    }

    /**
     * Execute the statement using the specified parameters against QLDB and retrieve the result.
     *
     * @param statement
     *              The PartiQL statement to be executed against QLDB.
     * @param parameters
     *              The parameters to be used with the PartiQL statement, for each ? placeholder in the statement.
     *
     * @return The result of executing the statement.
     * @throws IllegalStateException if this driver has been closed.
     * @throws com.amazonaws.AmazonClientException if there is an error executing against QLDB.
     */
    @Override
    public Result execute(String statement, IonValue... parameters) {
        try (QldbSession qldbSession = getSession()) {
            return qldbSession.execute(statement, parameters);
        }
    }

    /**
     * Execute the statement using the specified parameters against QLDB and retrieve the result.
     *
     * @param statement
     *              The PartiQL statement to be executed against QLDB.
     * @param retryIndicator
     *              A lambda that which is invoked when the Executor lambda is about to be retried due to a retriable
     *              error. Can be null if not applicable.
     * @param parameters
     *              The parameters to be used with the PartiQL statement, for each ? placeholder in the statement.
     *
     * @return The result of executing the statement.
     * @throws IllegalStateException if this driver has been closed.
     * @throws com.amazonaws.AmazonClientException if there is an error executing against QLDB.
     */
    @Override
    public Result execute(String statement, RetryIndicator retryIndicator, IonValue... parameters) {
        try (QldbSession qldbSession = getSession()) {
            return qldbSession.execute(statement, retryIndicator, parameters);
        }
    }

    /**
     * Execute the Executor lambda against QLDB within a transaction where no result is expected.
     *
     * @param executor
     *              A lambda with no return value representing the block of code to be executed within the transaction.
     *              This cannot have any side effects as it may be invoked multiple times.
     *
     * @throws AbortException if the Executor lambda calls {@link TransactionExecutor#abort()}.
     * @throws IllegalStateException if this driver has been closed.
     * @throws com.amazonaws.AmazonClientException if there is an error executing against QLDB.
     */
    @Override
    public void execute(ExecutorNoReturn executor) {
        try (QldbSession qldbSession = getSession()) {
            qldbSession.execute(executor);
        }
    }

    /**
     * Execute the Executor lambda against QLDB within a transaction where no result is expected.
     *
     * @param executor
     *              A lambda with no return value representing the block of code to be executed within the transaction.
     *              This cannot have any side effects as it may be invoked multiple times.
     * @param retryIndicator
     *              A lambda that which is invoked when the Executor lambda is about to be retried due to a retriable
     *              error. Can be null if not applicable.
     *
     * @throws AbortException if the Executor lambda calls {@link TransactionExecutor#abort()}.
     * @throws IllegalStateException if this driver has been closed.
     * @throws com.amazonaws.AmazonClientException if there is an error executing against QLDB.
     */
    @Override
    public void execute(ExecutorNoReturn executor, RetryIndicator retryIndicator) {
        try (QldbSession qldbSession = getSession()) {
            qldbSession.execute(executor, retryIndicator);
        }
    }

    /**
     * Execute the Executor lambda against QLDB and retrieve the result within a transaction.
     *
     * @param executor
     *              A lambda representing the block of code to be executed within the transaction. This cannot have any
     *              side effects as it may be invoked multiple times, and the result cannot be trusted until the
     *              transaction is committed.
     *
     * @return The return value of executing the executor. Note that if you directly return a {@link Result}, this will
     *         be automatically buffered in memory before the implicit commit to allow reading, as the commit will close
     *         any open results. Any other {@link Result} instances created within the executor block will be
     *         invalidated, including if the return value is an object which nests said {@link Result} instances within
     *         it.
     * @throws AbortException if the Executor lambda calls {@link TransactionExecutor#abort()}.
     * @throws IllegalStateException if this driver has been closed.
     * @throws com.amazonaws.AmazonClientException if there is an error executing against QLDB.
     */
    @Override
    public <T> T execute(Executor<T> executor) {
        try (QldbSession qldbSession = getSession()) {
            return qldbSession.execute(executor);
        }
    }

    /**
     * Execute the Executor lambda against QLDB and retrieve the result within a transaction.
     *
     * @param executor
     *              A lambda representing the block of code to be executed within the transaction. This cannot have any
     *              side effects as it may be invoked multiple times, and the result cannot be trusted until the
     *              transaction is committed.
     * @param retryIndicator
     *              A lambda that which is invoked when the Executor lambda is about to be retried due to a retriable
     *              error. Can be null if not applicable.
     *
     * @return The return value of executing the executor. Note that if you directly return a {@link Result}, this will
     *         be automatically buffered in memory before the implicit commit to allow reading, as the commit will close
     *         any open results. Any other {@link Result} instances created within the executor block will be
     *         invalidated, including if the return value is an object which nests said {@link Result} instances within
     *         it.
     * @throws AbortException if the Executor lambda calls {@link TransactionExecutor#abort()}.
     * @throws IllegalStateException if this driver has been closed.
     * @throws com.amazonaws.AmazonClientException if there is an error executing against QLDB.
     */
    @Override
    public <T> T execute(Executor<T> executor, RetryIndicator retryIndicator) {
        try (QldbSession qldbSession = getSession()) {
            return qldbSession.execute(executor, retryIndicator);
        }
    }

    /**
     * <p>Get a {@link QldbSession} object.</p>
     *
     * <p>This will attempt to retrieve an active existing session, or it will start a new session with QLDB unless the
     * number of allocated sessions has exceeded the pool size limit. If so, then it will continue trying to retrieve an
     * active existing session until the timeout is reached, throwing a {@link QldbClientException}.</p>
     *
     * @return The {@link QldbSession} object.
     *
     * @throws IllegalStateException if this driver has been closed.
     * @throws QldbClientException if a timeout is reached while attempting to retrieve a session.
     * @throws com.amazonaws.AmazonClientException if there is an error starting a session to QLDB.
     */
    public QldbSession getSession() {
        if (isClosed.get()) {
            logger.error(Errors.DRIVER_CLOSED.get());
            throw new IllegalStateException(Errors.DRIVER_CLOSED.get());
        }

        logger.debug("Getting session. There are {} free sessions; currently available permits is: {}.",
                pool.size(), poolPermits.availablePermits());

        try {
            if (poolPermits.tryAcquire(timeout, TimeUnit.MILLISECONDS)) {
                try {
                    for (QldbSessionImpl session = pool.poll(); session != null; session = pool.poll()) {
                        if (session.abortOrClose()) {
                            logger.debug("Reusing session from pool. Session ID: {}.", session.getSessionId());
                            return wrapSession(session);
                        }
                    }

                    QldbSessionImpl newSession = createNewSession();
                    logger.debug("Creating new pooled session. Session ID: {}.", newSession.getSessionId());
                    return wrapSession(newSession);
                } catch (final Exception e) {
                    // If creating a new session fails they don't use a permit!
                    poolPermits.release();
                    throw e;
                }
            } else {
                throw QldbClientException.create(String.format(Errors.SESSION_POOL_EMPTY.get(), timeout), logger);
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw QldbClientException.create(Errors.GET_SESSION_INTERRUPTED.get(), logger);
        }
    }

    /**
     * Create a new session to QLDB and use it to construct a new {@link QldbSessionImpl}.
     *
     * @return The newly created {@link QldbSessionImpl}.
     */
    private QldbSessionImpl createNewSession() {
        final Session session = Session.startSession(ledgerName, amazonQldbSession);
        return new QldbSessionImpl(session, retryLimit, readAhead, ionSystem, executorService);
    }

    /**
     * Return a session to the pool.
     *
     * @param session
     *                  The session to return to the pool.
     */
    private void releaseSession(QldbSessionImpl session) {
        pool.add(session);
        poolPermits.release();
        logger.debug("Session returned to pool; pool size is now: {}.", pool.size());
    }

    /**
     * Create a {@link PooledQldbSession} wrapped around a {@link QldbSessionImpl}.
     *
     * @param session
     *                  The session to wrap.
     *
     * @return The newly created {@link PooledQldbSession}.
     */
    private PooledQldbSession wrapSession(QldbSessionImpl session) {
        return new PooledQldbSession(session, this::releaseSession);
    }

    /**
     * Builder object for creating a {@link PooledQldbSession}, allowing for configuration of the parameters of
     * construction.
     */
    public static class PooledQldbDriverBuilder
            extends BaseSyncQldbDriverBuilder<PooledQldbDriverBuilder, PooledQldbDriver> {
        private int poolLimit = 0;
        private long timeout = DEFAULT_TIMEOUT_MS;

        /**
         * Restricted constructor. Use {@link #builder()} to retrieve an instance of this class.
         */
        protected PooledQldbDriverBuilder() {
        }

        /**
         * <p>Specify the limit to the pool of available sessions.</p>
         *
         * <p>Attempting to retrieve a session when the maximum number of sessions is already withdrawn will block until
         * a session becomes available. Set to 0 by default to use the maximum possible amount allowed by the client
         * builder's configuration.</p>
         *
         * @param poolLimit
         *              The maximum number of sessions that can be created from the pool at any one time. This amount
         *              cannot exceed the amount set in the {@link com.amazonaws.ClientConfiguration} of the
         *              {@link com.amazonaws.services.qldbsession.AmazonQLDBSessionClientBuilder} used for this builder.
         *
         * @return This builder object.
         */
        public PooledQldbDriverBuilder withPoolLimit(int poolLimit) {
            Validate.assertIsNotNegative(poolLimit, "poolLimit");
            this.poolLimit = poolLimit;
            return getSubclass();
        }

        /**
         * <p>Specify the timeout to wait for an available session to return to the pool in milliseconds.</p>
         *
         * <p>Calling {@link #getSession()} will wait until the timeout before throwing an exception if an available
         * session is still not returned to the pool.</p>
         *
         * @param timeout
         *              The maximum amount of time to wait, in milliseconds.
         *
         * @return This builder object.
         */
        public PooledQldbDriverBuilder withPoolTimeout(int timeout) {
            Validate.assertIsNotNegative(timeout, "timeout");
            this.timeout = timeout;
            return getSubclass();
        }

        @Override
        protected PooledQldbDriver createDriver() {
            if (0 == poolLimit) {
                poolLimit = clientMaxConnections;
            }
            Validate.assertPoolLimit(clientMaxConnections, poolLimit, "poolLimit");
            return new PooledQldbDriver(ledgerName, client, retryLimit, readAhead, poolLimit, timeout, ionSystem,
                    executorService);
        }
    }
}
