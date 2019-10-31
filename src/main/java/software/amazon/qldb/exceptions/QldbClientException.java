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
package software.amazon.qldb.exceptions;

import com.amazonaws.AmazonClientException;
import org.slf4j.Logger;

/**
 * Exception type representing exceptions that originate from the QLDB session, rather than QLDB itself.
 */
public class QldbClientException extends AmazonClientException {
    private static String SESSION_TOKEN_PREFIX = System.lineSeparator() + "Session token: ";

    /**
     * Protected constructor for creating an exception wrapping another exception.
     *
     * To create this exception, use one of the builder methods.
     *
     * @param cause
     *              The cause of this exception.
     */
    protected QldbClientException(Throwable cause) {
        super(cause);
    }

    /**
     * Protected constructor for creating an exception with a specific message.
     *
     * To create this exception, use one of the builder methods.
     *
     * @param message
     *              The message for this exception.
     */
    protected QldbClientException(String message) {
        super(message);
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
    protected QldbClientException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Factory method for creating an exception wrapping another exception.
     *
     * Will automatically log the exception on creation as well.
     *
     * @param cause
     *              The cause of this exception.
     * @param logger
     *              The logger to use when logging the exception.
     */
    public static QldbClientException create(Throwable cause, Logger logger) {
        logger.error(cause.getMessage());
        return new QldbClientException(cause);
    }

    /**
     * Factory method for creating an exception with a specific message.
     *
     * Will automatically log the exception on creation as well.
     *
     * @param message
     *              The message for this exception.
     * @param logger
     *              The logger to use when logging the exception.
     */
    public static QldbClientException create(String message, Logger logger) {
        logger.error(message);
        return new QldbClientException(message);
    }

    /**
     * Factory method for creating an exception with a specific message including the session token.
     *
     * Will automatically log the exception on creation as well.
     *
     * @param message
     *              The message for this exception.
     * @param logger
     *              The logger to use when logging the exception.
     */
    public static QldbClientException create(String message, String sessionToken, Logger logger) {
        final String errorMessage = message + SESSION_TOKEN_PREFIX + sessionToken;
        return create(errorMessage, logger);
    }

    /**
     * Factory method for creating an exception with a specific message wrapping another exception.
     *
     * Will automatically log the exception on creation as well.
     *
     * @param message
     *              The message for this exception.
     * @param cause
     *              The cause of this exception.
     * @param logger
     *              The logger to use when logging the exception.
     */
    public static QldbClientException create(String message, Throwable cause, Logger logger) {
        logger.error(message);
        return new QldbClientException(message, cause);
    }

    /**
     * Factory method for creating an exception with a specific message including the session token wrapping another exception.
     *
     * Will automatically log the exception on creation as well.
     *
     * @param message
     *              The message for this exception.
     * @param cause
     *              The cause of this exception.
     * @param logger
     *              The logger to use when logging the exception.
     */
    public static QldbClientException create(String message, String sessionToken, Throwable cause, Logger logger) {
        final String errorMessage = message + SESSION_TOKEN_PREFIX + sessionToken;
        return create(errorMessage, cause, logger);
    }
}
