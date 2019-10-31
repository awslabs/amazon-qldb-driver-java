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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazon.ion.IonValue;

import software.amazon.qldb.exceptions.AbortException;

/**
 * Transaction object used within lambda executions to provide a reduced view that allows only the operations that are
 * valid within the context of an active managed transaction.
 */
public class TransactionExecutor {
    private static final Logger logger = LoggerFactory.getLogger(TransactionExecutor.class);

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
        throw new AbortException();
    }

    /**
     * Execute the statement against QLDB and retrieve the result.
     *
     * @param statement
     *              The PartiQL statement to be executed against QLDB.
     *
     * @return The result of executing the statement.
     */
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
     */
    public Result execute(String statement, List<IonValue> parameters) {
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
