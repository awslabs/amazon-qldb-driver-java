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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazon.ion.IonSystem;
import com.amazonaws.annotation.ThreadSafe;
import com.amazonaws.services.qldbsession.AmazonQLDBSession;

import software.amazon.qldb.exceptions.Errors;
import software.amazon.qldb.exceptions.QldbClientException;

/**
 * <p>
 * Represents a factory for accessing pooled sessions to a specific ledger within QLDB. This class or {@link QldbDriver} should be
 * the main entry points to any interaction with QLDB. {@link #getSession()} will create a {@link PooledQldbSession} to the
 * specified ledger within QLDB as a communication channel. Any acquired sessions must be cleaned up with
 * {@link PooledQldbSession#close()} when they are no longer needed in order to return the session to the pool. If this is not
 * done, this driver may become unusable if the pool limit is exceeded.</p>
 *
 * <p>This factory pools sessions and attempts to return unused but available sessions when getting new sessions. The advantage to
 * using this over the non-pooling driver is that the underlying connection that sessions use to communicate with QLDB can be
 * recycled, minimizing resource usage by preventing unnecessary connections and reducing latency by not making unnecessary
 * requests to start new connections and end reusable, existing, ones.</p>
 *
 * <p>The pool does not remove stale sessions until a new session is retrieved. The default pool size is the maximum amount of
 * connections the session client allows. {@link #close()} should be called when this factory is no longer needed in order to
 * clean up resources, ending all sessions in the pool.</p>
 *
 * <p>This object is thread-safe.</p>
 */
@ThreadSafe
public class PooledQldbDriver extends BaseSyncQldbDriver {
    private static final Logger logger = LoggerFactory.getLogger(PooledQldbDriver.class);
    private static final long DEFAULT_TIMEOUT_MS = 30000;

    private final long timeout;
    private final Semaphore poolPermits;
    private final BlockingQueue<QldbSessionImpl> pool;

    protected PooledQldbDriver(String ledgerName, AmazonQLDBSession client, int retryLimit, int readAhead,
                               int poolLimit, long timeout, IonSystem ionSystem, ExecutorService executorService) {
        super(ledgerName, client, retryLimit, readAhead, ionSystem, executorService);

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
     * <p>Get a {@link QldbSession} object.</p>
     *
     * <p>This will attempt to retrieve an active existing session, or it will start a new session with QLDB unless the number of
     * allocated sessions has exceeded the pool size limit. If so, then it will continue trying to retrieve an active existing
     * session until the timeout is reached, throwing a {@link QldbClientException};</p>
     *
     * @return The {@link QldbSession} object.
     *
     * @throws IllegalStateException if this driver has been closed.
     * @throws QldbClientException if a timeout is reached while attempting to retrieve a session.
     * @throws com.amazonaws.AmazonClientException if there is an error communicating with QLDB.
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
                            logger.debug("Reusing session from pool.");
                            return wrapSession(session);
                        }
                    }

                    logger.debug("Creating new pooled session.");
                    return wrapSession(createNewSession());
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
        logger.debug("Session returned to pool; size is now: " + pool.size());
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
     * Builder object for creating a {@link PooledQldbSession}, allowing for configuration of the parameters of construction.
     */
    public static class PooledQldbDriverBuilder
            extends BaseSyncQldbDriverBuilder<PooledQldbDriverBuilder, PooledQldbDriver> {
        private int poolLimit = 0;
        private long timeout = DEFAULT_TIMEOUT_MS;

        /**
         * Restricted constructor. Use {@link #builder()} to retrieve an instance of this class.
         */
        protected PooledQldbDriverBuilder() {}

        /**
         * <p>Specify the limit to the pool of available sessions.</p>
         *
         * <p>Attempting to retrieve a session when the maximum number of sessions is already withdrawn will block until
         * a session becomes available. Set to 0 by default to use the maximum possible amount allowed by the client builder's
         * configuration.</p>
         *
         * @param poolLimit
         *              The maximum number of sessions that can be created from the pool at any one time. This amount cannot
         *              exceed the amount set in the {@link com.amazonaws.ClientConfiguration} of the
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
         * <p>Calling {@link #getSession()} will wait until the timeout before throwing an exception if an available session is still
         * not returned to the pool.</p>
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
            return new PooledQldbDriver(ledgerName, client, retryLimit, readAhead, poolLimit, timeout, ionSystem, executorService);
        }
    }
}
