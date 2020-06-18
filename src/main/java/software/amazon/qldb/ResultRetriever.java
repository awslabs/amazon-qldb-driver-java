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
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.qldbsession.model.Page;
import software.amazon.awssdk.utils.Validate;
import software.amazon.qldb.exceptions.Errors;
import software.amazon.qldb.exceptions.QldbDriverException;

/**
 * Used to retrieve the results from QLDB, either asynchronously or synchronously.
 *
 * If configured to read asynchronously, a thread will be created which will read ahead of the current position and
 * buffer chunks of data up to the configured readAhead setting. {@link #next()} will pause if the retrieval thread is
 * still retrieving the next chunk of data when it is needed.
 *
 * If configured to read synchronously, then the next chunk of data will be retrieved from QLDB during the call to
 * {@link #next()} when it has exhausted the current chunk of data.
 */
class ResultRetriever {
    private static final Logger logger = LoggerFactory.getLogger(ResultRetriever.class);

    private final Session session;
    private Page currentPage;
    private int currentResultValueIndex;

    private final Retriever retriever;
    private final IonSystem ionSystem;
    private final ExecutorService executorService;
    private final AtomicBoolean isClosed;

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
     * @param executorService
     *              The executor service to use for asynchronous retrieval. Null if new threads should be created.
     */
    ResultRetriever(Session session, Page firstPage, String txnId, int readAhead, IonSystem ionSystem,
                           ExecutorService executorService) {
        Validate.isNotNegative(readAhead, "readAhead");

        this.session = session;
        this.currentPage = firstPage;
        this.currentResultValueIndex = 0;
        this.ionSystem = ionSystem;
        this.executorService = executorService;
        this.isClosed = new AtomicBoolean(false);

        // Start the retriever thread if there are more chunks to retrieve.
        if (currentPage.nextPageToken() == null) {
            this.retriever = null;
        } else if (0 == readAhead) {
            this.retriever = new Retriever(session, txnId, currentPage.nextPageToken());
        } else {
            final ResultRetrieverRunnable runner = new ResultRetrieverRunnable(session, txnId,
                    currentPage.nextPageToken(), readAhead, isClosed);
            this.retriever = runner;

            if (null == executorService) {
                final Thread retrieverThread = new Thread(runner, "ResultRetriever");
                retrieverThread.setDaemon(true);
                retrieverThread.start();
            } else {
                this.executorService.submit(runner);
            }
        }
    }

    /**
     * Indicate if there are any more data to be retrieved.
     *
     * @return {@code true} if there is more data and {@link #next()} will return an IonValue; {@code false} otherwise.
     */
    public synchronized boolean hasNext() {
        if (isClosed.get()) {
            throw QldbDriverException.create(Errors.RESULT_PARENT_INACTIVE.get(), retriever.txnId);
        }
        while (currentResultValueIndex >= currentPage.values().size()) {
            if (null == currentPage.nextPageToken()) {
                return false;
            }
            currentPage = retriever.getNextPage();
            currentResultValueIndex = 0;
        }
        return true;
    }

    /**
     * Retrieve the next IonValue in the result. Note that this should only be called if {@link #hasNext()} returns
     * true.
     *
     * @return The next IonValue in the result.
     * @throws NoSuchElementException if the iteration has no more elements.
     */
    public synchronized IonValue next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        // TODO: Use an InputStream instead after the GitHub feature https://github.com/amzn/ion-java/issues/292 is resolved
        final byte[] bytes = currentPage.values().get(currentResultValueIndex++).ionBinary().asByteArray();
        return ionSystem.singleValue(bytes);
    }

    /**
     * Set this retriever to closed, indicating the parent transaction is closed.
     */
    void close() {
        isClosed.set(true);
    }

    /**
     * Basic object for retrieving the next chunk of data from QLDB for a result set.
     */
    private static class Retriever {
        final Session session;
        String nextPageToken;
        private final String txnId;

        /**
         * Constructor for creating the retriever for a specific session and result.
         *
         * @param session
         *              The parent session to use in retrieving results.
         * @param txnId
         *              The unique ID of the transaction.
         * @param nextPageToken
         *              The unique token identifying the next chunk of data to fetch for this result set.
         */
        private Retriever(Session session, String txnId, String nextPageToken) {
            this.session = session;
            this.txnId = txnId;
            this.nextPageToken = nextPageToken;
        }

        /**
         * Retrieve the next chunk of data from QLDB.
         *
         * @return The next chunk of data from QLDB.
         * @throws QldbDriverException if an unexpected error occurs during result retrieval.
         */
        Page getNextPage() {
            final Page result = session.sendFetchPage(txnId, nextPageToken).page();
            nextPageToken = result.nextPageToken();
            return result;
        }
    }

    private static class ResultRetrieverRunnable extends Retriever implements Runnable {
        private final BlockingDeque<ResultHolder<Exception>> results;
        private final int readAhead;
        private final AtomicBoolean isClosed;

        /**
         * Constructor for the Runnable responsible for asynchronous retrieval of results from QLDB.
         *
         * @param session
         *              The parent session to use in retrieving results.
         * @param txnId
         *              The unique ID of the transaction.
         * @param nextPageToken
         *              The unique token identifying the next chunk of data to fetch for this result set.
         * @param readAhead
         *              The maximum number of chunks of data to buffer at any one time.
         * @param isClosed
         *              {@link AtomicBoolean} tracking the state of the parent {@link ResultRetriever}.
         */
        ResultRetrieverRunnable(Session session, String txnId, String nextPageToken, int readAhead,
                                AtomicBoolean isClosed) {
            super(session, txnId, nextPageToken);
            this.readAhead = Math.min(1, readAhead - 1);
            this.results = new LinkedBlockingDeque<>(readAhead);
            this.isClosed = isClosed;
        }

        @Override
        public void run() {
            try {
                while (null != nextPageToken) {
                    final Page page = super.getNextPage();
                    try {
                        while (!results.offer(new ResultHolder(page), 50, TimeUnit.MILLISECONDS)) {
                            if (isClosed.get()) {
                                throw QldbDriverException.create(Errors.RESULT_PARENT_INACTIVE.get(),
                                                                 super.txnId);
                            }
                            Thread.yield();
                        }
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw QldbDriverException.create(Errors.RETRIEVE_INTERRUPTED.get(), ie);
                    }
                }
            } catch (Exception e) {
                results.clear();
                if (!results.offerFirst(new ResultHolder<>(e))) {
                    // We've failed to give back the exception; log it as a best case fallback.
                    logger.error(String.format(Errors.QUEUE_CAPACITY.get(), readAhead), e);
                }
            }
        }

        @Override
        Page getNextPage() {
            try {
                final ResultHolder<Exception> result = results.take();
                if (null != result.getAssociatedValue()) {
                    if (result.getAssociatedValue() instanceof RuntimeException) {
                        throw (RuntimeException) result.getAssociatedValue();
                    }
                    throw new RuntimeException(result.getAssociatedValue());
                }

                return result.getResult();
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw QldbDriverException.create(Errors.RETRIEVE_INTERRUPTED.get(), ie);
            }
        }
    }
}
