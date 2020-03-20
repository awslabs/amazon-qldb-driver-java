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

import com.amazon.ion.IonValue;
import com.amazonaws.annotation.NotThreadSafe;
import com.amazonaws.services.qldbsession.model.OccConflictException;
import java.util.List;

/**
 * <p>The top-level interface for a QldbSession object for interacting with QLDB. A QldbSession is linked to the
 * specified ledger in the parent driver of the instance of the QldbSession. In any given QldbSession, only one
 * transaction can be active at a time. This object can have only one underlying session to QLDB, and therefore the
 * lifespan of a QldbSession is tied to the underlying session, which is not indefinite, and on expiry this QldbSession
 * will become invalid, and a new QldbSession needs to be created from the parent driver in order to continue usage.</p>
 *
 * <p>When a QldbSession is no longer needed, {@link #close()} should be invoked in order to clean up any resources.</p>
 *
 * <p>See {@link PooledQldbDriver} for an example of session lifecycle management, allowing the re-use of sessions when
 * possible. There should only be one thread interacting with a session at any given time.</p>
 *
 * <p>There are three methods of execution, ranging from simple to complex; the first two are recommended for inbuilt
 * error handling:
 * <ul>
 * <li>{@link #execute(String, RetryIndicator, List)} and {@link #execute(String, RetryIndicator, IonValue...)} allow
 *      for a single statement to be executed within a transaction where the transaction is implicitly created and
 *      committed, and any recoverable errors are transparently handled. Each parameter besides the statement string
 *      have overloaded method variants where they are not necessary.</li>
 * <li>{@link #execute(Executor, RetryIndicator)} and {@link #execute(ExecutorNoReturn, RetryIndicator)} allow for
 *    more complex execution sequences where more than one execution can occur, as well as other method calls. The
 *    transaction is implicitly created and committed, and any recoverable errors are transparently handled.</li>
 *    {@link #execute(Executor)} and {@link #execute(ExecutorNoReturn)} are also available, providing the same
 *    functionality as the former two functions, but without a lambda function to be invoked upon a retryable
 *    error.</li>
 * <li>{@link #startTransaction()} allows for full control over when the transaction is committed and leaves the
 *    responsibility of OCC conflict handling up to the user. Transactions methods cannot be automatically retried, as
 *    the state of the transaction is ambiguous in the case of an unexpected error.</li>
 * </ul>
 * </p>
 */
@NotThreadSafe
public interface QldbSession extends AutoCloseable, RetriableExecutable {

    /**
     * Close the session, and clean up any resources. No-op if already closed.
     */
    @Override
    void close();

    /**
     * Retrieve the name of the ledger for this session.
     *
     * @return The ledger name for this session.
     */
    String getLedgerName();

    /**
     * Retrieve the session token of this session.
     *
     * @return The session token for this session.
     */
    String getSessionToken();

    /**
     * Retrieve the table names that are available within the ledger.
     *
     * @return The Iterable over the table names in the ledger.
     * @throws IllegalStateException if this QldbSessionImpl has been closed already, or if the transaction's commit
     *                               digest does not match the response from QLDB.
     * @throws OccConflictException if the number of retries has exceeded the limit and an OCC conflict occurs.
     * @throws com.amazonaws.AmazonClientException if there is an error communicating with QLDB.
     */
    Iterable<String> getTableNames();

    /**
     * Create a transaction object which allows for granular control over when a transaction is aborted or committed.
     *
     * @return The newly created transaction object.
     * @throws IllegalStateException if this QldbSessionImpl has been closed already.
     * @throws com.amazonaws.AmazonClientException if there is an error communicating with QLDB.
     */
    Transaction startTransaction();
}
