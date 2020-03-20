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
import com.amazonaws.AmazonClientException;
import com.amazonaws.annotation.NotThreadSafe;
import com.amazonaws.services.qldbsession.model.AmazonQLDBSessionException;
import com.amazonaws.services.qldbsession.model.InvalidSessionException;
import com.amazonaws.services.qldbsession.model.OccConflictException;
import com.amazonaws.services.qldbsession.model.StartTransactionResult;
import com.amazonaws.util.ValidationUtils;
import java.net.SocketTimeoutException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import org.apache.http.HttpStatus;
import org.apache.http.NoHttpResponseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.qldb.exceptions.AbortException;

/**
 * Represents a session to a specific ledger within QLDB, allowing for execution of PartiQL statements and
 * retrieval of the associated results, along with control over transactions for bundling multiple executions.
 *
 * The execute methods provided will automatically retry themselves in the case that an unexpected recoverable error
 * occurs, including OCC conflicts, by starting a brand new transaction and re-executing the statement within the new
 * transaction.
 *
 * There are three methods of execution, ranging from simple to complex:
 *  - {@link #execute(String, RetryIndicator, List)} and {@link #execute(String, RetryIndicator, IonValue...)} allow
 *    for a single statement to be executed within a transaction where the transaction is implicitly created and
 *    committed, and any recoverable errors are transparently handled. Each parameter besides the statement string
 *    have overloaded method variants where they are not necessary.
 *  - {@link #execute(Executor, RetryIndicator)} and {@link #execute(ExecutorNoReturn, RetryIndicator)} allow for
 *    more complex execution sequences where more than one execution can occur, as well as other method calls. The
 *    transaction is implicitly created and committed, and any recoverable errors are transparently handled.
 *    {@link #execute(Executor)} and {@link #execute(ExecutorNoReturn)} are also available, providing the same
 *    functionality as the former two functions, but without a lambda function to be invoked upon a retriable exception.
 *  - {@link #startTransaction()} allows for full control over when the transaction is committed and leaves the
 *    responsibility of OCC conflict handling up to the user. Transactions' methods cannot be automatically retried, as
 *    the state of the transaction is ambiguous in the case of an unexpected error.
 */
@NotThreadSafe
class QldbSessionImpl extends BaseQldbSession implements QldbSession {
    private static final Logger logger = LoggerFactory.getLogger(QldbSessionImpl.class);
    private final int readAhead;
    private final ExecutorService executorService;

    QldbSessionImpl(Session session, int retryLimit, int readAhead,
                    IonSystem ionSystem, ExecutorService executorService) {
        super(session, retryLimit, ionSystem);
        this.readAhead = readAhead;
        this.executorService = executorService;
    }

    /**
     * Close this session. No-op if already closed.
     */
    @Override
    public void close() {
        if (!isClosed.getAndSet(true)) {
            session.close();
        }
    }

    /**
     * Execute the statement against QLDB and retrieve the result.
     *
     * The execution occurs within a transaction which is implicitly committed, and any recoverable errors, including
     * OCC conflicts, are handled by starting a new transaction and re-executing the statement up to the retry limit
     * amount of times.
     *
     * As this function automatically retries on recoverable errors, if the statement is not idempotent or conditional
     * on a check within the statement itself, this may lead to unexpected results.
     *
     * If an {@link InvalidSessionException} is caught, it is considered a retriable exception by starting a new
     * {@link Session} to use for communicating with QLDB. Thus, as a side effect, this QldbSession can become valid
     * again despite a previous InvalidSessionException originating from other function calls on this object, or any
     * child {@link Transaction} and {@link Result} objects, when this function is invoked.
     *
     * @param statement
     *              The PartiQL statement to be executed against QLDB.
     *
     * @return The result of executing the statement.
     * @throws IllegalStateException if this QldbSession has been closed already, or if the transaction's commit digest
     *                               does not match the response from QLDB.
     * @throws OccConflictException if the number of retries has exceeded the limit and an OCC conflict occurs.
     * @throws com.amazonaws.AmazonClientException if there is an error executing against QLDB.
     */
    @Override
    public Result execute(String statement) {
        return execute(statement, Collections.emptyList());
    }

    /**
     * Execute the statement against QLDB and retrieve the result.
     *
     * The execution occurs within a transaction which is implicitly committed, and any recoverable errors, including
     * OCC conflicts, are handled by starting a new transaction and re-executing the statement up to the retry limit
     * amount of times.
     *
     * As this function automatically retries on recoverable errors, if the statement is not idempotent or conditional
     * on a check within the statement itself, this may lead to unexpected results.
     *
     * If an {@link InvalidSessionException} is caught, it is considered a retriable exception by starting a new
     * {@link Session} to use for communicating with QLDB. Thus, as a side effect, this QldbSession can become valid
     * again despite a previous InvalidSessionException originating from other function calls on this object, or any
     * child {@link Transaction} and {@link Result} objects, when this function is invoked.
     *
     * @param statement
     *              The PartiQL statement to be executed against QLDB.
     * @param retryIndicator
     *              A lambda that which is invoked when the Executor lambda is about to be retried due to a retriable
     *              error. Can be null if not applicable.
     *
     * @return The result of executing the statement.
     * @throws IllegalStateException if this QldbSession has been closed already, or if the transaction's commit digest
     *                               does not match the response from QLDB.
     * @throws OccConflictException if the number of retries has exceeded the limit and an OCC conflict occurs.
     * @throws com.amazonaws.AmazonClientException if there is an error executing against QLDB.
     */
    @Override
    public Result execute(String statement, RetryIndicator retryIndicator) {
        return execute(statement, retryIndicator, Collections.emptyList());
    }

    /**
     * Execute the statement using the specified parameters against QLDB and retrieve the result.
     *
     * The execution occurs within a transaction which is implicitly committed, and any recoverable errors, including
     * OCC conflicts, are handled by starting a new transaction and re-executing the statement up to the retry limit
     * amount of times.
     *
     * As this function automatically retries on recoverable errors, if the statement is not idempotent or conditional
     * on a check within the statement itself, this may lead to unexpected results.
     *
     * If an {@link InvalidSessionException} is caught, it is considered a retriable exception by starting a new
     * {@link Session} to use for communicating with QLDB. Thus, as a side effect, this QldbSession can become valid
     * again despite a previous InvalidSessionException originating from other function calls on this object, or any
     * child {@link Transaction} and {@link Result} objects, when this function is invoked.
     *
     * @param statement
     *              The PartiQL statement to be executed against QLDB.
     * @param parameters
     *              The parameters to be used with the PartiQL statement, for each ? placeholder in the statement.
     *
     * @return The result of executing the statement.
     * @throws IllegalStateException if this QldbSession has been closed already, or if the transaction's commit digest
     *                               does not match the response from QLDB.
     * @throws OccConflictException if the number of retries has exceeded the limit and an OCC conflict occurs.
     * @throws com.amazonaws.AmazonClientException if there is an error executing against QLDB.
     */
    @Override
    public Result execute(String statement, List<IonValue> parameters) {
        ValidationUtils.assertStringNotEmpty(statement, "statement");
        ValidationUtils.assertNotNull(parameters, "parameters");

        return execute(txn -> { return txn.execute(statement, parameters); }, null);
    }

    /**
     * Execute the statement using the specified parameters against QLDB and retrieve the result.
     *
     * The execution occurs within a transaction which is implicitly committed, and any recoverable errors, including
     * OCC conflicts, are handled by starting a new transaction and re-executing the statement up to the retry limit
     * amount of times.
     *
     * As this function automatically retries on recoverable errors, if the statement is not idempotent or conditional
     * on a check within the statement itself, this may lead to unexpected results.
     *
     * If an {@link InvalidSessionException} is caught, it is considered a retriable exception by starting a new
     * {@link Session} to use for communicating with QLDB. Thus, as a side effect, this QldbSession can become valid
     * again despite a previous InvalidSessionException originating from other function calls on this object, or any
     * child {@link Transaction} and {@link Result} objects, when this function is invoked.
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
     * @throws IllegalStateException if this QldbSession has been closed already, or if the transaction's commit digest
     *                               does not match the response from QLDB.
     * @throws OccConflictException if the number of retries has exceeded the limit and an OCC conflict occurs.
     * @throws com.amazonaws.AmazonClientException if there is an error executing against QLDB.
     */
    @Override
    public Result execute(String statement, RetryIndicator retryIndicator, List<IonValue> parameters) {
        return execute(txn -> { return txn.execute(statement, parameters); }, retryIndicator);
    }

    /**
     * Execute the statement using the specified parameters against QLDB and retrieve the result.
     *
     * The execution occurs within a transaction which is implicitly committed, and any recoverable errors, including
     * OCC conflicts, are handled by starting a new transaction and re-executing the statement up to the retry limit
     * amount of times.
     *
     * As this function automatically retries on recoverable errors, if the statement is not idempotent or conditional
     * on a check within the statement itself, this may lead to unexpected results.
     *
     * If an {@link InvalidSessionException} is caught, it is considered a retriable exception by starting a new
     * {@link Session} to use for communicating with QLDB. Thus, as a side effect, this QldbSession can become valid
     * again despite a previous InvalidSessionException originating from other function calls on this object, or any
     * child {@link Transaction} and {@link Result} objects, when this function is invoked.
     *
     * @param statement
     *              The PartiQL statement to be executed against QLDB.
     * @param parameters
     *              The parameters to be used with the PartiQL statement, for each ? placeholder in the statement.
     *
     * @return The result of executing the statement.
     * @throws IllegalStateException if this QldbSession has been closed already, or if the transaction's commit
     *                               digest does not match the response from QLDB.
     * @throws OccConflictException if the number of retries has exceeded the limit and an OCC conflict occurs.
     * @throws com.amazonaws.AmazonClientException if there is an error executing against QLDB.
     */
    @Override
    public Result execute(String statement, IonValue... parameters) {
        ValidationUtils.assertNotNull(parameters, "parameters");

        return execute(statement, Arrays.asList(parameters));
    }

    /**
     * Execute the statement using the specified parameters against QLDB and retrieve the result.
     *
     * The execution occurs within a transaction which is implicitly committed, and any recoverable errors, including
     * OCC conflicts, are handled by starting a new transaction and re-executing the statement up to the retry limit
     * amount of times.
     *
     * As this function automatically retries on recoverable errors, if the statement is not idempotent or conditional
     * on a check within the statement itself, this may lead to unexpected results.
     *
     * If an {@link InvalidSessionException} is caught, it is considered a retriable exception by starting a new
     * {@link Session} to use for communicating with QLDB. Thus, as a side effect, this QldbSession can become valid
     * again despite a previous InvalidSessionException originating from other function calls on this object, or any
     * child {@link Transaction} and {@link Result} objects, when this function is invoked.
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
     * @throws IllegalStateException if this QldbSession has been closed already, or if the transaction's commit
     *                               digest does not match the response from QLDB.
     * @throws OccConflictException if the number of retries has exceeded the limit and an OCC conflict occurs.
     * @throws com.amazonaws.AmazonClientException if there is an error executing against QLDB.
     */
    @Override
    public Result execute(String statement, RetryIndicator retryIndicator, IonValue... parameters) {
        ValidationUtils.assertNotNull(parameters, "parameters");

        return execute(statement, retryIndicator, Arrays.asList(parameters));
    }

    /**
     * Execute the Executor lambda against QLDB within a transaction where no result is expected.
     *
     * The execution occurs within a transaction which is implicitly committed, and any recoverable errors, including
     * OCC conflicts, are handled by starting a new transaction and re-executing the statement up to the retry limit
     * amount of times.
     *
     * As this function automatically retries on recoverable errors, if the statements are not idempotent or conditional
     * on a check within the executor, this may lead to unexpected results.
     *
     * If an {@link InvalidSessionException} is caught, it is considered a retriable exception by starting a new
     * {@link Session} to use for communicating with QLDB. Thus, as a side effect, this QldbSession can become valid
     * again despite a previous InvalidSessionException originating from other function calls on this object, or any
     * child {@link Transaction} and {@link Result} objects, when this function is invoked.
     *
     * @param executor
     *              A lambda with no return value representing the block of code to be executed within the transaction.
     *              This cannot have any side effects as it may be invoked multiple times.
     *
     * @throws AbortException if the Executor lambda calls {@link TransactionExecutor#abort()}.
     * @throws IllegalStateException if this QldbSession has been closed already, or if the transaction's commit
     *                               digest does not match the response from QLDB.
     * @throws OccConflictException if the number of retries has exceeded the limit and an OCC conflict occurs.
     * @throws com.amazonaws.AmazonClientException if there is an error executing against QLDB.
     */
    @Override
    public void execute(ExecutorNoReturn executor) {
        execute(executor, null);
    }

    /**
     * Execute the Executor lambda against QLDB within a transaction where no result is expected.
     *
     * The execution occurs within a transaction which is implicitly committed, and any recoverable errors, including
     * OCC conflicts, are handled by starting a new transaction and re-executing the statement up to the retry limit
     * amount of times.
     *
     * As this function automatically retries on recoverable errors, if the statements are not idempotent or conditional
     * on a check within the executor, this may lead to unexpected results.
     *
     * If an {@link InvalidSessionException} is caught, it is considered a retriable exception by starting a new
     * {@link Session} to use for communicating with QLDB. Thus, as a side effect, this QldbSession can become valid
     * again despite a previous InvalidSessionException originating from other function calls on this object, or any
     * child {@link Transaction} and {@link Result} objects, when this function is invoked.
     *
     * @param executor
     *              A lambda with no return value representing the block of code to be executed within the transaction.
     *              This cannot have any side effects as it may be invoked multiple times.
     * @param retryIndicator
     *              A lambda that which is invoked when the Executor lambda is about to be retried due to a retriable
     *              error. Can be null if not applicable.
     *
     * @throws AbortException if the Executor lambda calls {@link TransactionExecutor#abort()}.
     * @throws IllegalStateException if this QldbSession has been closed already, or if the transaction's commit
     *                               digest does not match the response from QLDB.
     * @throws OccConflictException if the number of retries has exceeded the limit and an OCC conflict occurs.
     * @throws com.amazonaws.AmazonClientException if there is an error executing against QLDB.
     */
    @Override
    public void execute(ExecutorNoReturn executor, RetryIndicator retryIndicator) {
        ValidationUtils.assertNotNull(executor, "executor");

        execute(txn -> {
            executor.execute(txn);
            return Boolean.TRUE;
        }, retryIndicator);
    }

    /**
     * Execute the Executor lambda against QLDB and retrieve the result within a transaction.
     *
     * The execution occurs within a transaction which is implicitly committed, and any recoverable errors, including
     * OCC conflicts, are handled by starting a new transaction and re-executing the statement up to the retry limit
     * amount of times. The Result of the Executor lambda cannot be deemed accurate until this method completes.
     *
     * As this function automatically retries on recoverable errors, if the statements are not idempotent or conditional
     * on a check within the executor, this may lead to unexpected results.
     *
     * If an {@link InvalidSessionException} is caught, it is considered a retriable exception by starting a new
     * {@link Session} to use for communicating with QLDB. Thus, as a side effect, this QldbSession can become valid
     * again despite a previous InvalidSessionException originating from other function calls on this object, or any
     * child {@link Transaction} and {@link Result} objects, when this function is invoked.
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
     * @throws IllegalStateException if this QldbSession has been closed already, or if the transaction's commit
     *                               digest does not match the response from QLDB.
     * @throws OccConflictException if the number of retries has exceeded the limit and an OCC conflict occurs.
     * @throws com.amazonaws.AmazonClientException if there is an error executing against QLDB.
     */
    @Override
    public <T> T execute(Executor<T> executor) {
        return execute(executor, null);
    }

    /**
     * Execute the Executor lambda against QLDB and retrieve the result within a transaction.
     *
     * The execution occurs within a transaction which is implicitly committed, and any recoverable errors, including
     * OCC conflicts, are handled by starting a new transaction and re-executing the statement up to the retry limit
     * amount of times. The Result of the Executor lambda cannot be deemed accurate until this method completes.
     *
     * As this function automatically retries on recoverable errors, if the statements are not idempotent or conditional
     * on a check within the executor, this may lead to unexpected results.
     *
     * If an {@link InvalidSessionException} is caught, it is considered a retriable exception by starting a new
     * {@link Session} to use for communicating with QLDB. Thus, as a side effect, this QldbSession can become valid
     * again despite a previous InvalidSessionException originating from other function calls on this object, or any
     * child {@link Transaction} and {@link Result} objects, when this function is invoked.
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
     * @throws IllegalStateException if this QldbSession has been closed already, or if the transaction's commit
     *                               digest does not match the response from QLDB.
     * @throws OccConflictException if the number of retries has exceeded the limit and an OCC conflict occurs.
     * @throws com.amazonaws.AmazonClientException if there is an error executing against QLDB.
     */
    @Override
    public <T> T execute(Executor<T> executor, RetryIndicator retryIndicator) {
        throwIfClosed();
        ValidationUtils.assertNotNull(executor, "executor");

        for (int executionAttempt = 0; true;) {
            Transaction transaction = null;
            try {
                transaction = startTransaction();
                T returnedValue = executor.execute(new TransactionExecutor(transaction));
                if (returnedValue instanceof StreamResult) {
                    // If someone accidentally returned a StreamResult object which would become invalidated by the
                    // commit, automatically buffer it to allow them to use the result anyway.
                    returnedValue = (T) new BufferedResult((Result) returnedValue);
                }
                transaction.commit();
                return returnedValue;
            } catch (AbortException e) {
                noThrowAbort(transaction);
                throw e;
            } catch (InvalidSessionException ise) {
                if (executionAttempt >= retryLimit) {
                    throw ise;
                }
                logger.info("Creating a new session to QLDB; previous session is no longer valid.", ise);
                session = Session.startSession(session.getLedgerName(), session.getClient());
            } catch (OccConflictException oce) {
                logger.info("OCC conflict occurred.", oce);
                if (executionAttempt >= retryLimit) {
                    throw oce;
                }
            } catch (AmazonQLDBSessionException se) {
                noThrowAbort(transaction);

                // If the error is an internal server error, or service unavailable, retry the transaction. QLDB
                // considers these exceptions to be non-fatal and retriable. Otherwise, bubble the exception up to
                // the user.
                if ((executionAttempt >= retryLimit) || ((se.getStatusCode() != HttpStatus.SC_INTERNAL_SERVER_ERROR) &&
                        (se.getStatusCode() != HttpStatus.SC_SERVICE_UNAVAILABLE))) {
                    throw se;
                }
            } catch (AmazonClientException ace) {
                noThrowAbort(transaction);

                // Retry if it's a timeout or a no response error.
                if ((executionAttempt >= retryLimit) || (!(ace.getCause() instanceof NoHttpResponseException ||
                        ace.getCause() instanceof SocketTimeoutException))) {
                    throw ace;
                }
            }

            ++executionAttempt;
            if (null != retryIndicator) {
                retryIndicator.onRetry(executionAttempt);
            }

            // There was a non-fatal error that occurred, so sleep for a bit before retry.
            retrySleep(executionAttempt);
        }
    }

    @Override
    public Iterable<String> getTableNames() {
        final Result result = execute(TABLE_NAME_QUERY);
        return new TableNameIterable(result);
    }

    @Override
    public Transaction startTransaction() {
        throwIfClosed();

        final StartTransactionResult startTransaction = session.sendStartTransaction();
        return new TransactionImpl(session, startTransaction.getTransactionId(), readAhead, ionSystem, executorService);
    }

    /**
     * Determine if the session is alive by sending an abort message. Will close the session if the abort is
     * unsuccessful.
     *
     * This should only be used when the session is known to not be in use, otherwise the state will be abandoned.
     *
     * @return {@code true} if the session was aborted successfully; {@code false} if the session is closed.
     */
    boolean abortOrClose() {
        if (isClosed.get()) {
            return false;
        }
        try {
            session.sendAbort();
            return true;
        } catch (AmazonClientException e) {
            isClosed.set(true);
            return false;
        }
    }

    /**
     * Retrieve the session ID of this session.
     *
     * @return The session ID for this session.
     */
    String getSessionId() {
        return this.session.getId();
    }

    /**
     * Send an abort which will not throw on failure, except in the case of an IllegalSessionException.
     *
     * @param transaction
     *                  The transaction to abort.
     */
    private void noThrowAbort(Transaction transaction) {
        try {
            if (null == transaction) {
                session.sendAbort();
            } else {
                transaction.abort();
            }
        } catch (AmazonClientException ace) {
            logger.warn("Ignored error aborting transaction during execution.", ace);
        }
    }
}
