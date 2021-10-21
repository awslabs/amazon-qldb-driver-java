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
import com.amazon.ion.IonValue;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.annotations.NotThreadSafe;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.qldbsessionv2.model.ExecuteStatementResult;
import software.amazon.awssdk.utils.Validate;
import software.amazon.qldb.exceptions.Errors;
import software.amazon.qldb.exceptions.QldbDriverException;

/**
 * <p>Interface that represents an active transaction with QLDB.</p>
 *
 *
 * <p>Every transaction is tied to the parent {@link QldbSession}, meaning that if the parent session is closed or
 * invalidated, the child transaction is automatically closed and cannot be used. Only one transaction can be active at
 * any given time per parent session, and thus every transaction should call {@link #abort()} or {@link #commit()} when
 * it is no longer needed, or when a new transaction is wanted from the parent session.</p>
 *
 * <p>An {@link software.amazon.awssdk.services.qldbsession.model.InvalidSessionException} indicates that the parent session is
 * expired, and a new transaction cannot be created without a new {@link QldbSession} being created from the parent
 * driver.</p>
 *
 * <p>Any unexpected errors that occur within a transaction should not be retried using the same transaction, as the
 * state of the transaction is now ambiguous.</p>
 *
 * <p>When an OCC conflict occurs, the transaction is closed and must be handled manually by creating a new transaction
 * and re-executing the desired queries.</p>
 *
 * <p>Child Result objects will be closed when the transaction is aborted or committed.</p>
 */
@NotThreadSafe
class Transaction {
    private static final Logger logger = LoggerFactory.getLogger(Transaction.class);
    private final Session session;
    private final String txnId;
    private final IonSystem ionSystem;

    private final int readAheadBufferCount;
    private final ExecutorService executorService;

    // We are allowing for an unbounded list here as the number of results in a transaction will practically be limited
    // by the operations performed on QLDB.
    private final Deque<StreamResult> results;

    Transaction(Session session, String txnId, int readAheadBufferCount, IonSystem ionSystem,
                ExecutorService executorService) {
        Validate.notNull(session, "session");
        Validate.notNull(txnId, "txnId");
        Validate.isNotNegative(readAheadBufferCount, "readAheadBufferCount");
        this.session = session;
        this.txnId = txnId;
        this.ionSystem = ionSystem;
        this.readAheadBufferCount = readAheadBufferCount;
        this.executorService = executorService;
        this.results = new ArrayDeque<>();
    }

    /**
     * Abort the transaction and roll back any changes. Any open Results created by the transaction will be closed.
     *
     * @throws software.amazon.awssdk.core.exception.SdkException if there is an error communicating with QLDB.
     */
    void abort() {
        try {
            internalClose();
            session.sendAbort().get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw QldbDriverException.create(Errors.GET_COMMAND_RESULT_INTERRUPTED.get());
        } catch (ExecutionException e) {
            throw (SdkException)e.getCause();
        }
    }

    /**
     * <p>Commit the transaction. Any open {@link Result} created by the transaction will be closed.</p>
     *
     * <p>If QLDB detects that there has been an optimistic concurrency control conflict (failed validation check to
     * ensure no other committed transaction has modified data that was read) then an OccConflictException will be
     * thrown.</p>
     *
     * @throws IllegalStateException if the transaction has been committed or aborted already, or if the returned commit
     *                               digest from QLDB does not match.
     * @throws RuntimeException if an Error event has been detected within the transaction.
     *
     * @throws software.amazon.awssdk.core.exception.SdkException if there is an error communicating with QLDB.
     */
    void commit() {
        try {
            session.sendCommit(txnId).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw QldbDriverException.create(Errors.GET_COMMAND_RESULT_INTERRUPTED.get());
        } catch (ExecutionException e) {
            throw (SdkException)e.getCause();
        }
    }

    Result execute(String statement) throws ExecutionException {
        return execute(statement, Collections.emptyList());
    }

    Result execute(String statement, List<IonValue> parameters) throws ExecutionException {
        software.amazon.awssdk.utils.Validate.paramNotBlank(statement, "statement");
        Validate.notNull(parameters, "parameters");

        final ExecuteStatementResult executeStatementResult;
        try {
            executeStatementResult = (ExecuteStatementResult) session.sendExecute(statement, parameters, txnId).get();
            final StreamResult result = new StreamResult(session, executeStatementResult, txnId,
                    readAheadBufferCount, ionSystem, executorService);
            results.add(result);
            return result;
        } catch (InterruptedException ie) {
            throw QldbDriverException.create(Errors.GET_COMMAND_RESULT_INTERRUPTED.get());
        }
    }

    Result execute(String statement, IonValue... parameters) throws ExecutionException {
        Validate.notNull(parameters, "parameters");

        return execute(statement, Arrays.asList(parameters));
    }

    /**
     * Stop retrieval threads for any child {@link StreamResult} objects.
     */
    void internalClose() {
        while (!results.isEmpty()) {
            // Avoid the use of forEach to guard against potential concurrent modification issues.
            (results.pop()).close();
        }
    }

    /**
     * Get the ID of the current transaction.
     *
     * @return The ID of the current transaction.
     */
    String getTransactionId() {
        return txnId;
    }
}
