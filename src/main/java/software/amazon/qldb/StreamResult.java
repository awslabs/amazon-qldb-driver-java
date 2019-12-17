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
import com.amazonaws.annotation.NotThreadSafe;
import com.amazonaws.services.qldbsession.model.Page;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.qldb.exceptions.Errors;

import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

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
class StreamResult extends BaseResult implements Result {
    private static final Logger logger = LoggerFactory.getLogger(StreamResult.class);

    private final AtomicBoolean isRetrieved;
    private final IonIterator childItr;
    private final boolean isEmpty;

    /**
     * @param session
     *              The parent session that represents the communication channel to QLDB.
     * @param firstPage
     *              The first chunk of the result, returned by the initial execution.
     * @param txnId
     *              The unique ID of the transaction.
     * @param readAheadBufferCount
     *              The number of buffers to asynchronously read ahead, 0 for synchronous retrieval.
     * @param ionSystem
     *              The Ion System to use for this object.
     * @param executorService
     *              The executor service to use for asynchronous retrieval. Null if new threads should be created.
     */
    public StreamResult(Session session, Page firstPage, String txnId, int readAheadBufferCount,
                        IonSystem ionSystem, ExecutorService executorService) {
        super(session, txnId, ionSystem);
        this.isRetrieved = new AtomicBoolean(false);
        this.childItr = new IonIterator(session, firstPage, txnId, readAheadBufferCount, ionSystem, executorService);
        this.isEmpty = !childItr.hasNext();
    }

    @Override
    public boolean isEmpty() {
        return isEmpty;
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
         * @param firstPage
         *              The first chunk of the result, returned by the initial execution.
         * @param txnId
         *              The unique ID of the transaction.
         * @param readAhead
         *              The number of buffers to asynchronously read ahead, 0 for synchronous retrieval.
         * @param ionSystem
         *              The Ion System to use for this object.
         * @param executorService
         *              The executor service to use for asynchronous retrieval. Null if new threads should be created.
         */
        IonIterator(Session session, Page firstPage, String txnId, int readAhead, IonSystem ionSystem,
                    ExecutorService executorService) {
            Validate.assertIsNotNegative(readAhead, "readAhead");
            this.retriever = new ResultRetriever(session, firstPage, txnId, readAhead, ionSystem,
                    executorService);
        }

        /**
         * Get boolean indicating if there is a next value in the iterator.
         *
         * @return Boolean indicating whether or not there is a next value.
         * @throws com.amazonaws.AmazonClientException if there is an error communicating with QLDB, when trying to get
         *                                             the next page of results.
         */
        @Override
        public boolean hasNext() {
            return retriever.hasNext();
        }

        /**
         * Get the next value in the iterator.
         *
         * @return The next IonValue resulting from the execution statement.
         * @throws com.amazonaws.AmazonClientException if there is an error communicating with QLDB, when trying to get
         *                                             the next page of results.
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
