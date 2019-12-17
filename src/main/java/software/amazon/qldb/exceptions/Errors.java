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
 * Enum identifying the possible errors within the QLDB session, allowing for retrieval of a localized error message.
 */
public enum Errors {
    DRIVER_CLOSED,
    GET_SESSION_INTERRUPTED,
    INCORRECT_TYPE,
    QUEUE_CAPACITY,
    RESULT_PARENT_INACTIVE,
    RETRIEVE_INTERRUPTED,
    SERIALIZING_PARAMS,
    SESSION_CLOSED,
    SESSION_POOL_EMPTY,
    STREAM_RESULT_ITERATED,
    TXN_CLOSED,
    TXN_DIGEST_MISMATCH;

    private static final ResourceBundle messages = ResourceBundle.getBundle("errors");

    /**
     * Retrieve the localized error message associated with the enum value.
     *
     * @return The associated localized error message.
     */
    public String get() {
        return messages.getString(this.name());
    }
}