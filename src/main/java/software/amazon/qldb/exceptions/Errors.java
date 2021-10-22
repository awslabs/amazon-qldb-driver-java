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

import java.util.ResourceBundle;

/**
 * Enum identifying the possible error types when executing a transaction.
 *
 * <p><b>Note</b>: this enum is for internal use only.</p>
 */
public enum Errors {
    ABORTED_EXCEPTION,         // txn aborted
    GET_SESSION_INTERRUPTED, // illegal state
    DRIVER_CLOSED,           // illegal state
    QUEUE_CAPACITY,
    RETRIEVE_INTERRUPTED,   // illegal state
    RESULT_PARENT_INACTIVE, // illegal state
    STREAM_RESULT_ITERATED, // illegal state
    CREATE_SESSION_INTERRUPTED, // illegal state
    GET_COMMAND_RESULT_INTERRUPTED, // illegal state
    GET_CONNECTION_INTERRUPTED, // illegal state
    SUBSCRIBER_ILLEGAL, // illegal state

    NO_SESSION_AVAILABLE,   // driver problem
    TXN_DIGEST_MISMATCH,    // driver problem
    INCORRECT_TYPE,         // driver problem
    SERIALIZING_PARAMS,     // driver problem
    SESSION_STREAM_ALREADY_OPEN, // driver problem
    SESSION_STREAM_NOT_EXIST, // driver problem
    SUBSCRIBER_TERMINATE, // driver problem
    FUTURE_QUEUE_EMTPY, // driver problem

    GENERIC_EXCEPTION;


    private static final ResourceBundle MESSAGES = ResourceBundle.getBundle("errors");

    /**
     * Retrieve the localized error message associated with the enum value.
     *
     * @return The associated localized error message.
     */
    public String get() {
        return MESSAGES.getString(this.name());
    }
}