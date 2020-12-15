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
import com.amazon.ion.IonValue;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.annotations.NotThreadSafe;
import software.amazon.awssdk.services.qldbsession.model.ExecuteStatementResult;
import software.amazon.awssdk.utils.Validate;
import software.amazon.qldb.exceptions.Errors;

/**
 * Result implementation which streams data from QLDB, discarding chunks as they are read.
 *
 * The StreamResult may be configured to asynchronously buffer chunks of data from QLDB before they are consumed by
 * the user of the StreamResult by altering the read-ahead property.
 *
 * Note that due to the fact that a result can only be retrieved from QLDB once, the StreamResult may only be iterated
 * over once. Attempts to do so multiple times will result in an exception.
 *
 * This implementation should be used by default to avoid excess memory consumption and to improve performance.
 */
@NotThreadSafe
class StreamResult implements Result {
    private static final Logger logger = LoggerFactory.getLogger(StreamResult.class);

    private final AtomicBoolean isRetrieved;
    private final IonIterator childItr;
    private final boolean isEmpty;
    private final Session session;
    private final String txnId;
    private final IonSystem ionSystem;

    /**
     * @param session
     *              The parent session that represents the communication channel to QLDB.
     * @param statementResult
     *              The result of the statement execution.
     * @param txnId
     *              The unique ID of the transaction.
     * @param readAheadBufferCount
     *              The number of buffers to asynchronously read ahead, 0 for synchronous retrieval.
     * @param ionSystem
     *              The Ion System to use for this object.
     * @param executorService
     *              The executor service to use for asynchronous retrieval. Null if new threads should be created.
     */
    StreamResult(Session session, ExecuteStatementResult statementResult, String txnId, int readAheadBufferCount,
                        IonSystem ionSystem, ExecutorService executorService) {
        this.session = session;
        this.txnId = txnId;
        this.ionSystem = ionSystem;
        this.isRetrieved = new AtomicBoolean(false);
        this.childItr = new IonIterator(session, statementResult, txnId, readAheadBufferCount, ionSystem, executorService);
        this.isEmpty = !childItr.hasNext();
    }

    @Override
    public boolean isEmpty() {
        return isEmpty;
    }

    /**
     * Gets the IOUsage statistics for the current statement.
     *
     * @return The current IOUsage statistics.
     */
    @Override
    public IOUsage getConsumedIOs() {
        return childItr.retriever.getIOUsage();
    }

    /**
     * Gets the server side timing information for the current statement.
     *
     * @return The current TimingInformation statistics.
     */
    @Override
    public TimingInformation getTimingInformation() {
        return childItr.retriever.getTimingInformation();
    }

    @Override
    public Iterator<IonValue> iterator() {
        if (isRetrieved.getAndSet(true)) {
            logger.error(Errors.STREAM_RESULT_ITERATED.get());
            throw new IllegalStateException(Errors.STREAM_RESULT_ITERATED.get());
        }

        return childItr;
    }

    /**
     * Explicitly release resources held by the result.
     */
    void close() {
        this.childItr.close();
    }

    /**
     * Object which allows for iteration over the individual IonValues that make up the whole result of a statement
     * execution against QLDB.
     */
    private static class IonIterator implements Iterator<IonValue> {
        private final ResultRetriever retriever;

        /**
         * Constructor.
         *
         * @param session
         *              The parent session that represents the communication channel to QLDB.
         * @param statementResult
         *              The result of the statement execution.
         * @param txnId
         *              The unique ID of the transaction.
         * @param readAhead
         *              The number of buffers to asynchronously read ahead, 0 for synchronous retrieval.
         * @param ionSystem
         *              The Ion System to use for this object.
         * @param executorService
         *              The executor service to use for asynchronous retrieval. Null if new threads should be created.
         */
        IonIterator(Session session, ExecuteStatementResult statementResult, String txnId, int readAhead, IonSystem ionSystem,
                    ExecutorService executorService) {
            Validate.isNotNegative(readAhead, "readAhead");

            software.amazon.awssdk.services.qldbsession.model.IOUsage consumedIOs = statementResult.consumedIOs();
            IOUsage ioUsage = (consumedIOs != null) ? new IOUsage(consumedIOs) : null;

            software.amazon.awssdk.services.qldbsession.model.TimingInformation timingInformation =
                statementResult.timingInformation();
            TimingInformation timingInfo = (timingInformation != null) ? new TimingInformation(timingInformation) : null;

            this.retriever = new ResultRetriever(session, statementResult.firstPage(), txnId, readAhead, ionSystem,
                    executorService, ioUsage, timingInfo);
        }

        /**
         * Get boolean indicating if there is a next value in the iterator.
         *
         * @return Boolean indicating whether or not there is a next value.
         * @throws software.amazon.awssdk.core.exception.SdkException if there is an error communicating with QLDB,
         *      when trying to get the next page of results.
         */
        @Override
        public boolean hasNext() {
            return retriever.hasNext();
        }

        /**
         * Get the next value in the iterator.
         *
         * @return The next IonValue resulting from the execution statement.
         * @throws software.amazon.awssdk.core.exception.SdkException if there is an error communicating with QLDB,
         *      when trying to get the next page of results.
         */
        @Override
        public IonValue next() {
            return retriever.next();
        }

        /**
         * Explicitly release and resources held by the result.
         */
        void close() {
            this.retriever.close();
        }
    }
}
