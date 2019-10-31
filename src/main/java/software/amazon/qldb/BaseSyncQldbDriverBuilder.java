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
package software.amazon.qldb;

import java.util.concurrent.ExecutorService;

import com.amazonaws.util.ValidationUtils;

/**
 * Base builder object for creating synchronous drivers, allowing for configuration of the parameters
 * of construction.
 */
abstract class BaseSyncQldbDriverBuilder<B extends BaseSyncQldbDriverBuilder, T extends BaseSyncQldbDriver>
        extends BaseQldbDriverBuilder<B, T> {
    protected static final String VERSION = "QLDB Driver for Java v";
    protected static final int DEFAULT_READAHEAD = 0;
    protected int readAhead = DEFAULT_READAHEAD;
    protected ExecutorService executorService;

    /**
     * Restricted constructor.
     */
    protected BaseSyncQldbDriverBuilder() {}

    /**
     * Specify the number of read-ahead buffers, determining the amount of sets of results buffered in memory,
     * for each open result set, created within the driver. If read-ahead is desired to be enabled, this must be set to at least 2.
     *
     * The higher the read-ahead buffer count, the more memory will be consumed by the driver when retrieving results.
     *
     * When read-ahead is set to any number greater than 0, a background thread will be started to perform retrieval.
     * To supply an {@link ExecutorService} for the threads, see {@link #withReadAhead(int, ExecutorService)}.
     *
     * When the executor is not provided, a new {@link Thread} is created to perform the retrieval.
     *
     * @param readAhead
     *              The number of read-ahead buffers for each open result set, 0 for no asynchronous read-ahead.
     *
     * @return This builder object.
     */
    public B withReadAhead(int readAhead) {
        Validate.assertIsAtLeastTwo(readAhead, "readAhead");
        this.readAhead = readAhead;
        this.executorService = null;
        return getSubclass();
    }

    /**
     * Specify the number of read-ahead buffers, determining the amount of sets of results buffered in memory,
     * for each open result set, created within the driver.
     *
     * The higher the read-ahead buffer count, the more memory will be consumed by the driver when retrieving results.
     *
     * When read-ahead is set to any number greater than 0, the supplied {@link ExecutorService} will be used to
     * asynchronously retrieved results. To simply start new threads, see {@link #withReadAhead(int)}.
     *
     * @param readAhead
     *              The number of read-ahead buffers for each open result set, 0 for no asynchronous read-ahead.
     * @param executorService
     *              The executor to be used by the retrieval thread.
     *
     * @return This builder object.
     */
    public B withReadAhead(int readAhead, ExecutorService executorService) {
        Validate.assertIsNotNegative(readAhead, "readAhead");
        ValidationUtils.assertNotNull(executorService, "executorService");
        this.readAhead = readAhead;
        this.executorService = executorService;
        return getSubclass();
    }

    @Override
    protected String getVersion() {
        return VERSION;
    }
}
