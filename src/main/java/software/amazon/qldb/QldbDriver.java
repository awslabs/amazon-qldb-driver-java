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

import java.util.concurrent.ExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazon.ion.IonSystem;
import com.amazonaws.annotation.ThreadSafe;
import com.amazonaws.services.qldbsession.AmazonQLDBSession;
import software.amazon.qldb.exceptions.Errors;

/**
 * <p>Represents a factory for accessing sessions to a specific ledger within QLDB. This class or {@link PooledQldbDriver} should be
 * the main entry points to any interaction with QLDB. {@link #getSession()} will create a {@link QldbSession} to the specified
 * ledger within QLDB as a communication channel. Any sessions acquired should be cleaned up with {@link QldbSession#close()} to
 * free up resources.</p>
 *
 * <p>This factory does not attempt to re-use or manage sessions in any way. It is recommended to use {@link PooledQldbDriver} for
 * both less resource usage and lower latency.</p>
 */
@ThreadSafe
public class QldbDriver extends BaseSyncQldbDriver {
    private static final Logger logger = LoggerFactory.getLogger(QldbDriver.class);

    protected QldbDriver(String ledgerName, AmazonQLDBSession amazonQLDBSession, int occRetryLimit, int readAhead,
                         IonSystem ionSystem, ExecutorService executorService) {
        super(ledgerName, amazonQLDBSession, occRetryLimit, readAhead, ionSystem, executorService);
    }

    /**
     * Retrieve a builder object for creating a {@link QldbDriver}.
     *
     * @return The builder object for creating a {@link QldbDriver}.
     */
    public static QldbDriverBuilder builder() {
        return new QldbDriverBuilder();
    }

    /**
     * <p>Create and return a newly instantiated {@link QldbSession} object.</p>
     *
     * <p>This will implicitly start a new session with QLDB.</p>
     *
     * @return The newly active {@link QldbSession} object.
     * @throws IllegalStateException if this driver has been closed.
     * @throws com.amazonaws.AmazonClientException if there is an error communicating with QLDB.
     */
    public QldbSession getSession() {
        if (isClosed.get()) {
            logger.error(Errors.DRIVER_CLOSED.get());
            throw new IllegalStateException(Errors.DRIVER_CLOSED.get());
        }

        logger.debug("Creating new session.");
        final Session session = Session.startSession(ledgerName, amazonQldbSession);
        return new QldbSessionImpl(session, retryLimit, readAhead, ionSystem, executorService);
    }

    /**
     * Builder object for creating a QldbDriver, allowing for configuration of the parameters of construction.
     */
    public static class QldbDriverBuilder extends BaseSyncQldbDriverBuilder<QldbDriverBuilder, QldbDriver> {
        /**
         * Restricted constructor. Use {@link #builder()} to retrieve an instance of this class.
         */
        QldbDriverBuilder() {}

        @Override
        protected QldbDriver createDriver() {
            return new QldbDriver(ledgerName, client, retryLimit, readAhead, ionSystem, executorService);
        }
    }
}
