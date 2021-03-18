/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package software.amazon.qldb.exceptions;

/**
 * Exception that contains the context of an error that occurred during a Execute lifecycle.
 *
 * <p><b>Note</b>: this class is for internal use only.</p>
 */
public class ExecuteException extends RuntimeException {
    private final boolean isRetryable;
    private final boolean isSessionAlive;
    private final boolean isInvalidSessionException;
    private final String transactionId;

    public ExecuteException(RuntimeException cause,
                            boolean isRetryable,
                            boolean isSessionAlive,
                            boolean isInvalidSessionException,
                            String transactionId) {
        super(cause);
        this.isRetryable = isRetryable;
        this.isSessionAlive = isSessionAlive;
        this.isInvalidSessionException = isInvalidSessionException;
        this.transactionId = transactionId;
    }

    @Override
    public RuntimeException getCause() {
        return (RuntimeException) super.getCause();
    }

    public boolean isRetryable() {
        return isRetryable;
    }

    public boolean isSessionAlive() {
        return isSessionAlive;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public boolean isInvalidSessionException() {
        return isInvalidSessionException;
    }
}
