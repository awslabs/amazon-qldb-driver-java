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

package software.amazon.qldbstreaming;

import com.amazon.ion.IonSystem;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.annotations.NotThreadSafe;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.qldbsessionv2.model.QldbSessionV2Exception;
import software.amazon.awssdk.services.qldbsessionv2.model.StartTransactionResult;
import software.amazon.qldbstreaming.exceptions.Errors;
import software.amazon.qldbstreaming.exceptions.ExecuteException;
import software.amazon.qldbstreaming.exceptions.QldbDriverException;
import software.amazon.qldbstreaming.exceptions.StatementException;
import software.amazon.qldbstreaming.exceptions.TransactionException;

/**
 * Object responsible for executing and maintaining the lifecycle of the transaction
 * while the {@link Executor} lambda is being executed.
 * <p>
 * If there is an error while starting a transaction during the execution of the {@link Executor}
 * lambda, then the lambda will be retried.
 */
@NotThreadSafe
class QldbSession {
    private static final Logger logger = LoggerFactory.getLogger(QldbSession.class);
    private final int readAhead;
    private final ExecutorService executorService;
    private Session session;

    private final IonSystem ionSystem;

    QldbSession(Session session, int readAhead, IonSystem ionSystem, ExecutorService executorService) {
        this.ionSystem = ionSystem;
        this.session = session;
        this.readAhead = readAhead;
        this.executorService = executorService;
    }

    void close() {
        this.session.close();
    }

    <T> T execute(Executor<T> executor) {
        Transaction txn = null;
        String txnId = "None";
        try {
            txn = this.startTransaction();
            txnId = txn.getTransactionId();
            T returnedValue = executor.execute(new TransactionExecutor(txn));
            if (returnedValue instanceof StreamResult) {
                // If someone accidentally returned a StreamResult object which would become invalidated by the
                // commit, automatically buffer it to allow them to use the result anyway.
                returnedValue = (T) new BufferedResult((Result) returnedValue);
            }
            txn.commit();
            return returnedValue;
        } catch (TransactionException | StatementException ee) {
            // TransactionException and StatementException should be retried with a new transaction
            throw new ExecuteException(
                ee,
                true,
                this.tryAbort(txn),
                false,
                txnId
            );
        } catch (SdkClientException sce) {
            // SdkClientException means that client couldn't reach out QLDB so transaction should be retried.
            // TODO: Retry transaction instead of giving up session.
            throw new ExecuteException(
                sce,
                true,
                false,
                false,
                txnId
            );
        } catch (QldbSessionV2Exception qse) {
            // Give up dead session. Retry with a new session when it's 500/503.
            boolean retryable = (qse.statusCode() == HttpStatus.SC_INTERNAL_SERVER_ERROR)
                || (qse.statusCode() == HttpStatus.SC_SERVICE_UNAVAILABLE);
            throw new ExecuteException(
                qse,
                retryable,
                false,
                false,
                txnId);
        } catch (RuntimeException re) {
            throw new ExecuteException(
                re,
                false,
                this.tryAbort(txn),
                false,
                txnId
            );
        } finally {
            if (txn != null) {
                txn.internalClose();
            }
        }
    }

    String getSessionId() {
        return this.session.getId();
    }

    private Transaction startTransaction() {
        final StartTransactionResult startTransaction;
        try {
            startTransaction = (StartTransactionResult) session.sendStartTransaction().get();
            return new Transaction(session, startTransaction.transactionId(), readAhead, ionSystem, executorService);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw QldbDriverException.create(Errors.GET_COMMAND_RESULT_INTERRUPTED.get());
        } catch (ExecutionException ee) {
            throw (SdkException) ee.getCause();
        }
    }

    private boolean tryAbort(Transaction txn) {
        try {
            if (txn == null) {
                this.session.sendAbort();
            } else {
                txn.abort();
            }
            return true;
        } catch (Exception e) {
            logger.warn("Ignored error aborting transaction during execution.", e);
            return false;
        }
    }
}
