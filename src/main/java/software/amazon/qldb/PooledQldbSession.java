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

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import com.amazon.ion.IonValue;
import com.amazonaws.annotation.NotThreadSafe;
import com.amazonaws.services.qldbsession.model.InvalidSessionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.qldb.exceptions.Errors;

/**
 * Represents a pooled session object. See {@link QldbSessionImpl} for more details.
 */
@NotThreadSafe
final class PooledQldbSession implements QldbSession {
    private static final Logger logger = LoggerFactory.getLogger(PooledQldbSession.class);

    private final QldbSessionImpl session;
    private final CloseSessionAction closeAction;
    private final AtomicBoolean isClosed;

    @FunctionalInterface
    interface CloseSessionAction {
        void invoke(QldbSessionImpl session);
    }

    PooledQldbSession(QldbSessionImpl session, CloseSessionAction closeAction) {
        this.session = session;
        this.closeAction = closeAction;
        this.isClosed = new AtomicBoolean(false);
    }

    @Override
    public void close() {
        if (!isClosed.getAndSet(true)) {
            closeAction.invoke(session);
        }
    }

    @Override
    public Result execute(String statement) {
        return invokeOnSession(() -> session.execute(statement));
    }

    @Override
    public Result execute(String statement, List<IonValue> parameters) {
        return invokeOnSession(() -> session.execute(statement, parameters));
    }

    @Override
    public void execute(ExecutorNoReturn executor) {
        invokeOnSession(() -> {
            session.execute(executor);
            return null;
        });
    }

    @Override
    public void execute(ExecutorNoReturn executor, RetryIndicator retryIndicator) {
        invokeOnSession(() -> {
            session.execute(executor, retryIndicator);
            return null;
        });
    }

    @Override
    public <T extends Object> T execute(Executor<T> executor) {
        return invokeOnSession(() -> session.execute(executor));
    }

    @Override
    public <T extends Object> T execute(Executor<T> executor, RetryIndicator retryIndicator) {
        return invokeOnSession(() -> session.execute(executor, retryIndicator));
    }

    @Override
    public String getLedgerName() {
        throwIfClosed();
        return session.getLedgerName();
    }

    @Override
    public String getSessionToken() {
        throwIfClosed();
        return session.getSessionToken();
    }

    @Override
    public Iterable<String> getTableNames() {
        return invokeOnSession(session::getTableNames);
    }

    @Override
    public Transaction startTransaction() {
        return invokeOnSession(session::startTransaction);
    }

    /**
     * Check and throw if this session is closed.
     *
     * @throws IllegalStateException if this session is closed.
     */
    private void throwIfClosed() {
        if (isClosed.get()) {
            logger.error(Errors.SESSION_CLOSED.get());
            throw new IllegalStateException(Errors.SESSION_CLOSED.get());
        }
    }

    /**
     * Handle method calls using the internal {@link QldbSessionImpl} this object wraps.
     *
     * @param sessionFunction
     *                      The function to call on the session.
     *
     * @return The result of the function call to the session.
     * @throws IllegalStateException if this session is closed.
     * @throws InvalidSessionException if the {@link Session} to QLDB used by this session is invalid.
     */
    private <T extends Object> T invokeOnSession(Supplier<T> sessionFunction) {
        throwIfClosed();
        try {
            return sessionFunction.get();
        } catch (InvalidSessionException ise) {
            close();
            throw ise;
        }
    }
}
