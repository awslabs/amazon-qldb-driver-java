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
import software.amazon.qldb.QldbDriver;

/**
 * Exception thrown when an attempt is made to start another transaction
 * while the previous transaction was still open.
 *
 * <p>When this exception occurs the driver will retry the lambda function passed to
 * any of the {@link QldbDriver}'s execute methods.
 * </p>
 *
 * <p><b>Note</b>: this enum is for internal use only.</p>
 */
public class TransactionAlreadyOpenException extends QldbDriverException {
    public TransactionAlreadyOpenException(QldbSessionException cause) {
        super(cause);
    }
}
