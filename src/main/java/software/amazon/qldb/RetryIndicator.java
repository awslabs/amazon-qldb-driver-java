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
 * Represents a function that is invoked when a recoverable error, including OCC conflicts, occurs and the retry limit
 * has not yet been exceeded, indicating that a new transaction is about to be created and the Executor lambda
 * re-invoked. The retry attempt is passed in as a parameter.
 */
@FunctionalInterface
public interface RetryIndicator {
    /**
     * Executes the operation when a retry is about to occur.
     *
     * @param retryNumber
     *              The number of times this has previously been invoked.
     */
    void onRetry(int retryNumber);
}
