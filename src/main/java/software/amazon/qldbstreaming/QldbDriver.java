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

package software.amazon.qldbstreaming;

import software.amazon.qldbstreaming.exceptions.TransactionAbortedException;

/**
 * <p>The driver for Amazon QLDB.</p>
 *
 * <p>The main goal of the driver is to receive lambda functions that are passed to any of its execute methods.
 * The code in these lambda functions should be idempotent as the lambda function might be
 * called more than once based on the TransactionRetryPolicy.
 * </p>
 *
 * <h3>Idempotency:</h3>
 * <p>A transaction needs to be idempotent to avoid undesirable side effects.</p>
 *
 * <p>For example, consider the below transaction which inserts a document into People table. It first checks if the document
 * already exists in the table or not. So even if this transaction is executed multiple times, it will not cause any side
 * effects.</p>
 *
 * <p>Without this check, the transaction might duplicate documents in the table if it is retried. A retry may happen when the
 * transaction commits successfully on QLDB server side, but the driver/client timeouts waiting for a response.</p>
 *
 * For example, the following code shows a lambda that is retryable:
 * <pre>{@code
 * driver.execute(txn -> {
 *     Result result = txn.execute("SELECT firstName, age, lastName FROM People WHERE firstName = 'Bob'");
 *     boolean recordExists = result.iterator().hasNext();
 *     if (recordExists) {
 *        txn.execute("UPDATE People SET age = 32 WHERE firstName = 'Bob'");
 *     } else {
 *        txn.execute("INSERT INTO People {'firstName': 'Bob', 'lastName': 'Doe', 'age':32}");
 *     }
 * });
 * }</pre>
 *
 * <h3>Instantiating the driver</h3>
 * The driver can be instantiated using the {@link QldbDriverBuilder}. Example:
 *
 * <pre>{@code
 *     QldbSessionClientBuilder sessionClientBuilder = QldbSessionClient.builder();
 *
 *     QldbDriverBuilder builder = QldbDriver
 *          .builder()
 *          .ledger(ledger);
 *          .maxConcurrentTransactions(poolLimit)
 *          .transactionRetryPolicy(RetryPolicy.maxRetries(3));
 *          .sessionClientBuilder(sessionClientBuilder)
 *          .build();
 * }</pre>
 *
 *
 *
 */
public interface QldbDriver extends AutoCloseable {
    /**
     * Execute the Executor lambda against QLDB within a transaction where no result is expected.
     *
     * @param executor
     *              A lambda with no return value representing the block of code to be executed within the transaction.
     *              This cannot have any side effects as it may be invoked multiple times.
     *
     * @throws software.amazon.awssdk.core.exception.SdkException if there is an error executing against QLDB.
     */
    void execute(ExecutorNoReturn executor);

    /**
     * <p>Execute the Executor lambda against QLDB within a transaction where no result is expected.</p>
     *
     * This method accepts a RetryPolicy that overrides the RetryPolicy set when creating the driver. Use it
     * to customize the backoff delay used when retrying transactions.
     *
     * @param executor
     *              A lambda with no return value representing the block of code to be executed within the transaction.
     *              This cannot have any side effects as it may be invoked multiple times.
     *
     * @param retryPolicy
     *              A {@link RetryPolicy} that overrides the RetryPolicy set when creating the driver. The given retry policy
     *              will be used when retrying the transaction.
     * @throws TransactionAbortedException if the Executor lambda calls {@link TransactionExecutor#abort()}.
     * @throws software.amazon.awssdk.core.exception.SdkException if there is an error executing against QLDB.
     */
    void execute(ExecutorNoReturn executor, RetryPolicy retryPolicy);

    /**
     * Execute the Executor lambda against QLDB and retrieve the result within a transaction.
     *
     * @param executor
     *              A lambda representing the block of code to be executed within the transaction. This cannot have any
     *              side effects as it may be invoked multiple times, and the result cannot be trusted until the
     *              transaction is committed.
     * @param <T>
     *         The type of value that will be returned.
     *
     * @return The return value of executing the executor. Note that if you directly return a {@link Result}, this will
     *         be automatically buffered in memory before the implicit commit to allow reading, as the commit will close
     *         any open results. Any other {@link Result} instances created within the executor block will be
     *         invalidated, including if the return value is an object which nests said {@link Result} instances within
     *         it.
     * @throws TransactionAbortedException if the Executor lambda calls {@link TransactionExecutor#abort()}.
     * @throws software.amazon.awssdk.core.exception.SdkException if there is an error executing against QLDB.
     */
    <T> T execute(Executor<T> executor);

    /**
     * Execute the Executor lambda against QLDB and retrieve the result within a transaction.
     *
     * This method accepts a RetryPolicy that overrides the RetryPolicy set when creating the driver. Use it
     * to customize the backoff delay used when retrying transactions.
     *
     * @param executor
     *              A lambda representing the block of code to be executed within the transaction. This cannot have any
     *              side effects as it may be invoked multiple times, and the result cannot be trusted until the
     *              transaction is committed.
     * @param retryPolicy
     *              A {@link RetryPolicy} that overrides the RetryPolicy set when creating the driver. The given retry policy
     *              will be used when retrying the transaction.
     *
     * @param <T>
     *         The type of value that will be returned.
     *
     * @return The return value of executing the executor. Note that if you directly return a {@link Result}, this will
     *         be automatically buffered in memory before the implicit commit to allow reading, as the commit will close
     *         any open results. Any other {@link Result} instances created within the executor block will be
     *         invalidated, including if the return value is an object which nests said {@link Result} instances within
     *         it.
     * @throws TransactionAbortedException if the Executor lambda calls {@link TransactionExecutor#abort()}.
     * @throws software.amazon.awssdk.core.exception.SdkException if there is an error executing against QLDB.
     */
    <T> T execute(Executor<T> executor, RetryPolicy retryPolicy);

    /**
     * Retrieve the table names that are available within the ledger.
     *
     * @return The Iterable over the table names in the ledger.
     * @throws IllegalStateException if this QldbSession has been closed already, or if the transaction's commit
     *                               digest does not match the response from QLDB.
     * @throws software.amazon.awssdk.core.exception.SdkException if there is an error communicating with QLDB.
     */
    Iterable<String> getTableNames();


    static QldbDriverBuilder builder() {
        return new QldbDriverImplBuilder();
    }
}
