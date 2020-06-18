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

package software.amazon.qldb.exceptions;

import software.amazon.awssdk.services.qldbsession.model.QldbSessionException;

/**
 * Exception used to represent all types of failures that can be thrown by the driver
 * that are not QLDB service side exceptions. These exceptions are related to the
 * driver not able to create a Qldb Hash or a not able to initialize the execution
 * of a transaction due to a client-side problem.
 *
 */
public class QldbDriverException extends QldbSessionException {
    private static String TXN_TOKEN_PREFIX = System.lineSeparator() + "TransactionId: ";

    /**
     * Protected constructor for creating an exception wrapping another exception.
     *
     * To create this exception, use one of the builder methods.
     *
     * @param cause
     *              The cause of this exception.
     */
    protected QldbDriverException(Throwable cause) {
        super(QldbSessionException.builder().cause(cause));
    }

    /**
     * Protected constructor for creating an exception with a specific message.
     *
     * To create this exception, use one of the builder methods.
     *
     * @param message
     *              The message for this exception.
     */
    protected QldbDriverException(String message) {
        super(QldbSessionException.builder().message(message));
    }

    /**
     * Protected constructor for creating an exception with a specific message wrapping another exception.
     *
     * To create this exception, use one of the builder methods.
     *
     * @param message
     *              The message for this exception.
     * @param cause
     *              The cause of this exception.
     */
    protected QldbDriverException(String message, Throwable cause) {
        super(QldbSessionException.builder().message(message).cause(cause));
    }

    /**
     * Factory method for creating an exception wrapping another exception.
     *
     * Will automatically log the exception on creation as well.
     *
     * @param cause
     *              The cause of this exception.
     * @return Instance of QldbDriverException with the original exception wrapped in.
     */
    public static QldbDriverException create(Throwable cause) {
        return new QldbDriverException(cause);
    }

    /**
     * Factory method for creating an exception with a specific message.
     *
     * @param message
     *              The message for this exception.
     * @return A QldbDriverException with the error message explaining the cause of the failure.
     */
    public static QldbDriverException create(String message) {
        return new QldbDriverException(message);
    }

    /**
     * Factory method for creating an exception with a specific message including the session token.
     *
     * @param message
     *              The message for this exception.
     * @param transactionId
     *              TransactionId that failed
     *
     * @return A QldbDriverException with the error message explaining the cause of the failure.
     */
    public static QldbDriverException create(String message, String transactionId) {
        final String errorMessage = message + TXN_TOKEN_PREFIX + transactionId;
        return create(errorMessage);
    }

    /**
     * Factory method for creating an exception with a specific message wrapping another exception.
     *
     *
     * @param message
     *              The message for this exception.
     * @param cause
     *              The cause of this exception.
     * @return A QldbDriverException with the error message and the cause of the transaction execution failure.
     */
    public static QldbDriverException create(String message, Throwable cause) {
        return new QldbDriverException(message, cause);
    }

}
