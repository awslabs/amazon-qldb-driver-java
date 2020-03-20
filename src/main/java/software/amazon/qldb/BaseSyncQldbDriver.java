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

import java.util.concurrent.ExecutorService;

/**
 * Represents a base class for a factory for creating synchronous sessions.
 */
abstract class BaseSyncQldbDriver extends BaseQldbDriver implements AutoCloseable {
    protected final int readAhead;
    protected final ExecutorService executorService;

    /**
     * Constructor for the base abstract class of a factory for creating synchronous sessions.
     *
     * @param ledgerName
     *                  The ledger to create sessions to.
     * @param amazonQldbSession
     *                  The low-level session used for communication with QLDB.
     * @param retryLimit
     *                  The amount of retries sessions created by this driver will attempt upon encountering a non-fatal
     *                  error.
     * @param readAhead
     *                  The number of read-ahead buffers for each open result set created from sessions from this
     *                  driver.
     * @param ionSystem
     *                  The {@link IonSystem} sessions created by this driver will use.
     * @param executorService
     *                  The executor to be used by the retrieval thread if read-ahead is enabled.
     */
    protected BaseSyncQldbDriver(String ledgerName, AmazonQLDBSession amazonQldbSession, int retryLimit, int readAhead,
                                 IonSystem ionSystem, ExecutorService executorService) {
        super(ledgerName, amazonQldbSession, retryLimit, ionSystem);
        this.readAhead = readAhead;
        this.executorService = executorService;
    }

    @Override
    public void close() {
        isClosed.set(true);
    }

    /**
     * Retrieve a {@link QldbSession} object.
     *
     * @return The {@link QldbSession} object.
     */
    public abstract QldbSession getSession();
}
