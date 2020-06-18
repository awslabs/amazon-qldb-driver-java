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
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.annotations.NotThreadSafe;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.qldbsession.model.BadRequestException;
import software.amazon.awssdk.services.qldbsession.model.InvalidSessionException;
import software.amazon.awssdk.services.qldbsession.model.OccConflictException;
import software.amazon.awssdk.services.qldbsession.model.QldbSessionException;
import software.amazon.awssdk.services.qldbsession.model.StartTransactionResult;
import software.amazon.awssdk.utils.Validate;
import software.amazon.qldb.exceptions.TransactionAbortedException;
import software.amazon.qldb.exceptions.TransactionAlreadyOpenException;

/**
 * Object responsible for executing and maintaining the lifecycle of the transaction
 * while the {@link Executor} lambda is being executed.
 *
 * If there is an error while starting a transaction during the execution of the {@link Executor}
 * lambda, then the lambda will be retried.
 *
 */
@NotThreadSafe
class QldbSession {
    private static final Logger logger = LoggerFactory.getLogger(QldbSession.class);
    private final int readAhead;
    private final ExecutorService executorService;
    private Session session;
    private final AtomicBoolean isClosed = new AtomicBoolean(true);
    private final IonSystem ionSystem;

    QldbSession(Session session, int readAhead, IonSystem ionSystem, ExecutorService executorService) {
        this.ionSystem = ionSystem;
        this.session = session;
        this.isClosed.set(false);
        this.readAhead = readAhead;
        this.executorService = executorService;
    }

    /**
     * Closes the object so it can't be reused to execute transactions.
     */
    void close() {
        if (!isClosed.getAndSet(true)) {
            session.close();
        }
    }

    boolean isClosed() {
        return isClosed.get();
    }

    <T> T execute(Executor<T> executor, RetryPolicy retryPolicy, ExecutionContext executionContext) {
        Validate.paramNotNull(executor, "executor");
        while (true) {
            executionContext.setLastException(null);
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
            } catch (TransactionAlreadyOpenException taoe) {
                noThrowAbort(transaction);
                executionContext.setLastException(taoe);
                if (executionContext.retryAttempts() >= retryPolicy.maxRetries()) {
                    throw (BadRequestException) taoe.getCause();
                }
                logger.debug("Retrying the transaction. {} ", taoe.getMessage());
            } catch (TransactionAbortedException ae) {
                noThrowAbort(transaction);
                throw ae;
            } catch (InvalidSessionException ise) {
                if (transaction != null) {
                    logger.warn("Transaction {} expired while executing. Cause {} ", transaction.getTransactionId(),
                                ise.getMessage());
                }
                isClosed.set(true);
                throw ise;
            } catch (OccConflictException occe) {
                executionContext.setLastException(occe);
                if (executionContext.retryAttempts() >= retryPolicy.maxRetries()) {
                    throw occe;
                }
                logger.info("Retrying the transaction. {} ", occe.getMessage());
            } catch (QldbSessionException qse) {
                executionContext.setLastException(qse);
                noThrowAbort(transaction);
                if ((executionContext.retryAttempts() >= retryPolicy.maxRetries())
                    || ((qse.statusCode() != HttpStatus.SC_INTERNAL_SERVER_ERROR)
                        && (qse.statusCode() != HttpStatus.SC_SERVICE_UNAVAILABLE))) {
                    throw qse;
                }
            } catch (SdkClientException sce) {
                executionContext.setLastException(sce);
                noThrowAbort(transaction);

                // SdkClientException means that client couldn't reach out QLDB so
                // transaction should be retried up to the max number of attempts.
                if (executionContext.retryAttempts() >= retryPolicy.maxRetries()) {
                    throw sce;
                }
            }
            executionContext.increaseAttempt();

            // There was a non-fatal error that occurred, so sleep for a bit before retry.
            retrySleep(executionContext, transaction, retryPolicy);
        }
    }

    String getSessionId() {
        return this.session.getId();
    }

    private Transaction startTransaction() {
        try {
            final StartTransactionResult startTransaction = session.sendStartTransaction();
            return new Transaction(session, startTransaction.transactionId(), readAhead, ionSystem, executorService);
        } catch (BadRequestException e) {
            throw new TransactionAlreadyOpenException(e);
        }
    }

    private void noThrowAbort(Transaction transaction) {
        try {
            if (null == transaction) {
                session.sendAbort();
            } else {
                transaction.abort();
            }
        } catch (SdkException se) {
            logger.warn("Ignored error aborting transaction during execution.", se);
        }
    }

    private void retrySleep(ExecutionContext executionContext, Transaction transaction, RetryPolicy retryPolicy) {
        try {
            final String transactionId = transaction != null ? transaction.getTransactionId() : null;
            final RetryPolicyContext retryPolicyContext = new RetryPolicyContext(executionContext.lastException(),
                                                                                 executionContext.retryAttempts(),
                                                                                 transactionId);
            Duration backoffDelay =
                retryPolicy.backoffStrategy().calculateDelay(retryPolicyContext);
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
