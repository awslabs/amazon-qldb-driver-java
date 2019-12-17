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

import com.amazon.ion.IonValue;
import com.amazonaws.annotation.NotThreadSafe;
import com.amazonaws.services.qldbsession.model.OccConflictException;

import java.util.List;

/**
 * <p>Interface that represents an active transaction with QLDB.</p>
 *
 *
 * <p>Every transaction is tied to the parent {@link QldbSession}, meaning that if the parent session is closed or
 * invalidated, the child transaction is automatically closed and cannot be used. Only one transaction can be active at
 * any given time per parent session, and thus every transaction should call {@link #abort()} or {@link #commit()} when
 * it is no longer needed, or when a new transaction is wanted from the parent session.</p>
 *
 * <p>An {@link com.amazonaws.services.qldbsession.model.InvalidSessionException} indicates that the parent session is
 * dead, and a new transaction cannot be created without a new {@link QldbSession} being created from the parent
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
public interface Transaction extends AutoCloseable {
    /**
     * Abort the transaction and roll back any changes. Any open Results created by the transaction will be closed.
     *
     * @throws com.amazonaws.AmazonClientException if there is an error communicating with QLDB.
     */
    void abort();

    /**
     * Clean up any resources, and abort the transaction if it has not already been committed or aborted.
     */
    void close();

    /**
     * <p>Commit the transaction. Any open {@link Result} created by the transaction will be closed.</p>
     *
     * <p>If QLDB detects that there has been an optimistic concurrency control conflict (failed validation check to
     * ensure no other committed transaction has modified data that was read) then an OccConflictException will be
     * thrown.</p>
     *
     * @throws IllegalStateException if the transaction has been committed or aborted already, or if the returned commit
     *                               digest from QLDB does not match.
     * @throws OccConflictException if an OCC conflict has been detected within the transaction.
     * @throws com.amazonaws.AmazonClientException if there is an error communicating with QLDB.
     */
    void commit();

    /**
     * Execute the statement against QLDB and retrieve the result.
     *
     * @param statement
     *              The PartiQL statement to be executed against QLDB.
     *
     * @return The result of executing the statement.
     * @throws IllegalStateException if the transaction has been committed or aborted already.
     * @throws com.amazonaws.AmazonClientException if there is an error communicating with QLDB.
     */
    Result execute(String statement);

    /**
     * Execute the statement using the specified parameters against QLDB and retrieve the result.
     *
     * @param statement
     *              The PartiQL statement to be executed against QLDB.
     * @param parameters
     *              The parameters to be used with the PartiQL statement, for each ? placeholder in the statement.
     *
     * @return The result of executing the statement.
     * @throws IllegalStateException if the transaction has been committed or aborted already.
     * @throws com.amazonaws.AmazonClientException if there is an error communicating with QLDB.
     */
    Result execute(String statement, List<IonValue> parameters);

    /**
     * Get the ID of the current transaction.
     *
     * @return The ID of the current transaction.
     */
    String getTransactionId();
}
