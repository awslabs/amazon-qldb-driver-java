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
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.qldb.exceptions.Errors;

/**
 * The abstract base session to a specific ledger within QLDB, containing the properties and methods shared by the
 * asynchronous and synchronous implementations of a session to a specific ledger within QLDB.
 */
abstract class BaseQldbSession {
    static final String TABLE_NAME_QUERY =
            "SELECT VALUE name FROM information_schema.user_tables WHERE status = 'ACTIVE'";
    private static final Logger logger = LoggerFactory.getLogger(BaseQldbSession.class);
    private static final long SLEEP_BASE_MS = 10;
    private static final long SLEEP_CAP_MS = 5000;

    final int retryLimit;
    Session session;
    final AtomicBoolean isClosed = new AtomicBoolean(true);
    final IonSystem ionSystem;

    BaseQldbSession(Session session, int retryLimit, IonSystem ionSystem) {
        this.retryLimit = retryLimit;
        this.ionSystem = ionSystem;
        this.session = session;
        this.isClosed.set(false);
    }

    /**
     * Retrieve the ledger name session is for.
     *
     * @return The ledger name this session is for.
     */
    public String getLedgerName() {
        return session.getLedgerName();
    }

    /**
     * Retrieve the session token of this session.
     *
     * @return The session token of this session.
     */
    public String getSessionToken() {
        return session.getToken();
    }

    /**
     * Determine if the driver's sessions are closed.
     *
     * @return {@code true} if the sessions are closed; {@code false} otherwise.
     */
    public boolean isClosed() {
        return isClosed.get();
    }

    /**
     * Implement an exponential backoff with jitter sleep.
     *
     *
     * @param attemptNumber
     *                  The attempt number for the retry, used for the exponential portion of the sleep.
     */
    static void retrySleep(int attemptNumber) {
        try {
            // Algorithm taken from https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
            final double jitterRand = Math.random();
            final double exponentialBackoff = Math.min(SLEEP_CAP_MS, (long) Math.pow(SLEEP_BASE_MS, attemptNumber));
            Thread.sleep((long) (jitterRand * (exponentialBackoff + 1)));
        } catch (InterruptedException e) {
            // Reset the interruption flag.
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Check and throw if the session is closed.
     *
     * @throws IllegalStateException if the session is closed.
     */
    void throwIfClosed() {
        if (isClosed.get()) {
            logger.error(Errors.SESSION_CLOSED.get());
            throw new IllegalStateException(Errors.SESSION_CLOSED.get());
        }
    }
}
