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
import java.util.List;
import software.amazon.qldb.exceptions.TransactionAbortedException;

/**
 * Transaction object used within lambda executions to provide a reduced view that allows only the operations that are
 * valid within the context of an active managed transaction.
 */
public class TransactionExecutor implements Executable {
    private final Transaction transaction;

    /**
     * Constructor.
     *
     * @param transaction
     *              The transaction object the TransactionExecutor wraps.
     */
    public TransactionExecutor(Transaction transaction) {
        this.transaction = transaction;
    }

    /**
     * Abort the transaction and roll back any changes.
     */
    public void abort() {
        throw new TransactionAbortedException();
    }

    /**
     * Execute the statement against QLDB and retrieve the result.
     *
     * @param statement
     *              The PartiQL statement to be executed against QLDB.
     *
     * @return The result of executing the statement.
     * @throws software.amazon.awssdk.services.qldbsession.model.QldbSessionException if there is an error executing against QLDB.
     */
    @Override
    public Result execute(String statement) {
        return transaction.execute(statement);
    }

    /**
     * Execute the statement using the specified parameters against QLDB and retrieve the result.
     *
     * @param statement
     *              The PartiQL statement to be executed against QLDB.
     * @param parameters
     *              The parameters to be used with the PartiQL statement, for each ? placeholder in the statement.
     *
     * @return The result of executing the statement.
     * @throws software.amazon.awssdk.services.qldbsession.model.QldbSessionException if there is an error executing against QLDB.
     */
    @Override
    public Result execute(String statement, List<IonValue> parameters) {
        return transaction.execute(statement, parameters);
    }

    /**
     * Execute the statement using the specified parameters against QLDB and retrieve the result.
     *
     * @param statement
     *              The PartiQL statement to be executed against QLDB.
     * @param parameters
     *              The parameters to be used with the PartiQL statement, for each ? placeholder in the statement.
     *
     * @return The result of executing the statement.
     * @throws software.amazon.awssdk.services.qldbsession.model.QldbSessionException if there is an error executing against QLDB.
     */
    @Override
    public Result execute(String statement, IonValue... parameters) {
        return transaction.execute(statement, parameters);
    }

    /**
     * Get the ID of the current transaction.
     *
     * @return The ID of the current transaction.
     */
    public String getTransactionId() {
        return transaction.getTransactionId();
    }
}
