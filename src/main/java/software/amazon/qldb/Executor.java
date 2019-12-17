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

/**
 * Represents a function that accepts a {@link TransactionExecutor} and returns a single result.
 *
 * @param <R>
 *          The type of the result of the {@link #execute(TransactionExecutor)} operation.
 */
@FunctionalInterface
public interface Executor<R> {
    /**
     * Executes the operation using the specified {@link TransactionExecutor}.
     *
     * @param executor
     *              The executor to use in executing any statements against QLDB.
     *
     * @return The result of the execution.
     */
    R execute(TransactionExecutor executor);
}
