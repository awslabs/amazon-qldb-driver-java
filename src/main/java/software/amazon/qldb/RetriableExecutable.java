/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
import software.amazon.qldb.exceptions.AbortException;

public interface RetriableExecutable extends Executable {
    /**
     * Execute the statement against QLDB and retrieve the result.
     *
     * @param statement
     *              The PartiQL statement to be executed against QLDB.
     * @param retryIndicator
     *              A lambda that which is invoked when the Executor lambda is about to be retried due to a retriable
     *              error. Can be null if not applicable.
     *
     * @return The result of executing the statement.
     * @throws com.amazonaws.AmazonClientException if there is an error executing against QLDB.
     */
    Result execute(String statement, RetryIndicator retryIndicator);

    /**
     * Execute the statement using the specified parameters against QLDB and retrieve the result.
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
     * @throws com.amazonaws.AmazonClientException if there is an error executing against QLDB.
     */
    Result execute(String statement, RetryIndicator retryIndicator, List<IonValue> parameters);

    /**
     * Execute the statement using the specified parameters against QLDB and retrieve the result.
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
     * @throws com.amazonaws.AmazonClientException if there is an error executing against QLDB.
     */
    Result execute(String statement, RetryIndicator retryIndicator, IonValue... parameters);

    /**
     * Execute the Executor lambda against QLDB within a transaction where no result is expected.
     *
     * @param executor
     *              A lambda with no return value representing the block of code to be executed within the transaction.
     *              This cannot have any side effects as it may be invoked multiple times.
     *
     * @throws com.amazonaws.AmazonClientException if there is an error executing against QLDB.
     */
    void execute(ExecutorNoReturn executor);

    /**
     * Execute the Executor lambda against QLDB within a transaction where no result is expected.
     *
     * @param executor
     *              A lambda with no return value representing the block of code to be executed within the transaction.
     *              This cannot have any side effects as it may be invoked multiple times.
     *
     * @param retryIndicator
     *              A lambda that which is invoked when the Executor lambda is about to be retried due to a retriable
     *              error. Can be null if not applicable.
     * @throws AbortException if the Executor lambda calls {@link TransactionExecutor#abort()}.
     * @throws com.amazonaws.AmazonClientException if there is an error executing against QLDB.
     */
    void execute(ExecutorNoReturn executor, RetryIndicator retryIndicator);

    /**
     * Execute the Executor lambda against QLDB and retrieve the result within a transaction.
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
     * @throws com.amazonaws.AmazonClientException if there is an error executing against QLDB.
     */
    <T> T execute(Executor<T> executor);

    /**
     * Execute the Executor lambda against QLDB and retrieve the result within a transaction.
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
     * @throws com.amazonaws.AmazonClientException if there is an error executing against QLDB.
     */
    <T> T execute(Executor<T> executor, RetryIndicator retryIndicator);
}
