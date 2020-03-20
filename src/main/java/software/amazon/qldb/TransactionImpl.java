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
import com.amazonaws.services.qldbsession.model.ExecuteStatementResult;
import com.amazonaws.services.qldbsession.model.OccConflictException;
import com.amazonaws.util.ValidationUtils;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.qldb.exceptions.Errors;


/**
 * Implementation of a QLDB transaction which also tracks child Results for the purposes of managing their lifecycle.
 *
 * Any unexpected errors that occur when calling methods in this class should not be retried, as the state of the
 * transaction is now ambiguous. When an OCC conflict occurs, the transaction is closed.
 *
 * Child Result objects will be closed when the transaction is aborted or committed.
 */
@NotThreadSafe
class TransactionImpl extends BaseTransaction implements Transaction {
    private static final Logger logger = LoggerFactory.getLogger(TransactionImpl.class);

    private final int readAheadBufferCount;
    private final ExecutorService executorService;

    // We are allowing for an unbounded list here as the number of results in a transaction will practically be limited
    // by the operations performed on QLDB.
    private final Deque<Result> results;

    TransactionImpl(Session session, String txnId, int readAheadBufferCount, IonSystem ionSystem,
                    ExecutorService executorService) {
        super(session, txnId, ionSystem);
        Validate.assertIsNotNegative(readAheadBufferCount, "readAheadBufferCount");
        this.readAheadBufferCount = readAheadBufferCount;
        this.executorService = executorService;
        this.results = new ArrayDeque<>();
    }

    @Override
    public void abort() {
        if (!isClosed.get()) {
            internalClose();
            session.sendAbort();
        }
    }

    @Override
    public void close() {
        try {
            abort();
        } catch (AmazonClientException ace) {
            logger.warn("Ignored error aborting transaction when closing.", ace);
        }
    }

    @Override
    public void commit() {
        throwIfClosed();

        try {
            final ByteBuffer hashByteBuffer = ByteBuffer.wrap(getTransactionHash().getQldbHash());
            final ByteBuffer commitDigest = session.sendCommit(txnId, hashByteBuffer).getCommitDigest();
            if (!commitDigest.equals(hashByteBuffer)) {
                logger.error(Errors.TXN_DIGEST_MISMATCH.get());
                throw new IllegalStateException(Errors.TXN_DIGEST_MISMATCH.get());
            }
        } catch (OccConflictException oce) {
            // Avoid sending courtesy abort since we know transaction is dead on OCC conflict.
            throw oce;
        } catch (AmazonClientException ace) {
            close();
            throw ace;
        } finally {
            internalClose();
        }
    }

    @Override
    public Result execute(String statement) {
        return execute(statement, Collections.emptyList());
    }

    @Override
    public Result execute(String statement, List<IonValue> parameters) {
        throwIfClosed();
        ValidationUtils.assertStringNotEmpty(statement, "statement");
        ValidationUtils.assertNotNull(parameters, "parameters");

        setTransactionHash(dot(getTransactionHash(), statement, parameters, ionSystem));
        final ExecuteStatementResult executeStatementResult = session.sendExecute(statement, parameters, txnId);
        final StreamResult result = new StreamResult(session, executeStatementResult.getFirstPage(), txnId,
                readAheadBufferCount, ionSystem, executorService);
        results.add(result);
        return result;
    }

    @Override
    public Result execute(String statement, IonValue... parameters) {
        ValidationUtils.assertNotNull(parameters, "parameters");

        return execute(statement, Arrays.asList(parameters));
    }

    /**
     * Mark the transaction as closed, and stop retrieval threads for any child {@link StreamResult} objects.
     */
    void internalClose() {
        isClosed.set(true);
        while (!results.isEmpty()) {
            // Avoid the use of forEach to guard against potential concurrent modification issues.
            ((StreamResult) results.pop()).close();
        }
    }
}
