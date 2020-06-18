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

import software.amazon.awssdk.core.exception.SdkException;

/**
 * Keeps track of the number of transaction executions and the exception that
 * caused the transaction to get retried.
 *  
 */
class ExecutionContext {
    private int retryAttempts = 0;
    private SdkException lastException = null;

    void increaseAttempt() {
        retryAttempts += 1;
    }

    void setLastException(SdkException lastException) {
        this.lastException = lastException;
    }

    int retryAttempts() {
        return retryAttempts;
    }

    SdkException lastException() {
        return lastException;
    }
}
