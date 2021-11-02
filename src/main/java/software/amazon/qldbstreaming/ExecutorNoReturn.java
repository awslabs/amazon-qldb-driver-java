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

/**
 * A functional interface that contains all the code to be executed in a QLDB transaction but doesn't return a value.
 *
 * <p>
 * The transaction commits after the function finishes executing all statements and if no exception is thrown inside the function.
 * </p>
 * <p>
 * If an exception is thrown then the transaction will be aborted.
 * </p>
 * <p>
 * The function will receive a {@link TransactionExecutor} to execute PartiQL statements.
 * </p>
 */
@FunctionalInterface
public interface ExecutorNoReturn {
    /**
     * Executes the operation using the specified {@link TransactionExecutor}.
     *
     * @param executor The executor to use in executing any statements against QLDB.
     */
    void execute(TransactionExecutor executor);
}
