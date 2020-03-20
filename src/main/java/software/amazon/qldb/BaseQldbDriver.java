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

import com.amazon.ion.IonSystem;
import com.amazonaws.services.qldbsession.AmazonQLDBSession;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Represents a base class for a factory that creates sessions.
 */
abstract class BaseQldbDriver {
    protected final String ledgerName;
    protected final AmazonQLDBSession amazonQldbSession;
    protected final int retryLimit;
    protected final IonSystem ionSystem;
    protected final AtomicBoolean isClosed;

    /**
     * Constructor for the base abstract class of a factory for creating sessions.
     *
     * @param ledgerName
     *                  The ledger to create sessions to.
     * @param amazonQldbSession
     *                  The low-level session used for communication with QLDB.
     * @param retryLimit
     *                  The amount of retries sessions created by this driver will attempt upon encountering a non-fatal
     *                  error.
     * @param ionSystem
     *                  The {@link IonSystem} sessions created by this driver will use.
     */
    protected BaseQldbDriver(String ledgerName, AmazonQLDBSession amazonQldbSession, int retryLimit,
                             IonSystem ionSystem) {
        this.ledgerName = ledgerName;
        this.amazonQldbSession = amazonQldbSession;
        this.retryLimit = retryLimit;
        this.ionSystem = ionSystem;
        this.isClosed = new AtomicBoolean(false);
    }
}
