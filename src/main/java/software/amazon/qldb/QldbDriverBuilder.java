/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
import java.util.concurrent.ExecutorService;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.services.qldbsessionv2.QldbSessionV2AsyncClientBuilder;

/**
 * <p>Builder class to create the {@link QldbDriver}.</p>
 *
 * The driver can be instantiated using a snippet like the one below:
 *
 * <pre>{@code
 *     QldbSessionClientBuilder sessionClientBuilder = QldbSessionClient.builder();
 *
 *     QldbDriver qldbDriver = QldbDriver
 *         .builder()
 *         .ledger(ledger)
 *         .maxConcurrentTransactions(poolLimit)
 *         .transactionRetryPolicy(TransactionRetryPolicy.builder().maxNumberOfRetries(retryLimit).build())
 *         .sessionClientBuilder(sessionClientBuilder)
 *         .httpClientBuilder(ApacheHttpClient.builder().maxConnections(maxConnections))
 *         .build();
 * }</pre>
 *
 */
public interface QldbDriverBuilder {
    /**
     * Build a {@link QldbDriver} instance using the current configuration set with the builder.
     *
     * @return A newly created {@link QldbDriver}.
     */
    QldbDriver build();

    /**
     * <p>
     * Specify the ledger to connect to and submit the transactions.
     * </p>
     *
     * <p>
     *     The ledgerName is mandatory.
     * </p>
     * @param ledgerName
     *              The name of the ledger to create a driver with.
     *
     * @return This builder object.
     */
    QldbDriverBuilder ledger(String ledgerName);

    /**
     * <p>Specify the low level QLDB session builder that should be used for accessing QLDB.</p>
     *
     * <p>Note that the user agent suffix and retry count will be set on the specified session builder,
     * and the http client will be overwritten. To pass a customized http client to the {@link QldbDriver},
     * use {@link #httpClientBuilder(SdkHttpClient.Builder)}.</p>
     *
     * <p>
     *     The clientBuilder is mandatory.
     * </p>
     *
     * @param clientBuilder
     *              The builder used to create the low-level session.
     *
     * @return This builder object.
     */
    QldbDriverBuilder sessionClientBuilder(QldbSessionV2AsyncClientBuilder clientBuilder);

    /**
     * <p>
     * Specify the {@link IonSystem} that should be used to generate the hash of
     * the parameters and the statements. If no parameter is specified then a default value would be used.
     * </p>
     *
     * <p>
     *     The ionSystem is optional.
     * </p>
     *
     * @param ionSystem
     *              The {@link IonSystem} to use.
     *
     * @return This builder object.
     */
    QldbDriverBuilder ionSystem(IonSystem ionSystem);

    /**
     * <p>
     * Specify the retry policy that will be used to execute the transactions. If no parameter is specified then an instance
     * of {@link DefaultQldbTransactionBackoffStrategy} would be used.
     * </p>
     * <p>
     *     The retryPolicy is optional.
     * </p>
     *
     * @param retryPolicy
     *              Contains the rules to retry the transaction.
     *
     * @return This builder object.
     */
    QldbDriverBuilder transactionRetryPolicy(RetryPolicy retryPolicy);



    /**
     * <p>Specify the maximum number of concurrent transactions that can be executed at any point in time. If no value is
     * specified then the value from AWS SDK default max connections would be used.</p>
     *
     * <p>
     *     The maxConcurrentTransactions is optional.
     * </p>
     *
     * @param maxConcurrentTransactions
     *              Specifies the maximum number of concurrent transactions that can be executed by the {@link QldbDriver}.
     *
     * @return This builder object.
     */
    QldbDriverBuilder maxConcurrentTransactions(int maxConcurrentTransactions);

    /**
     * <p>Specify the number of read-ahead buffers, determining the amount of sets of results buffered in memory,
     * for each open result set, created within the driver. If read-ahead is desired to be enabled, this must be set to
     * at least 2.</p>
     *
     * <p>The higher the read-ahead buffer count, the more memory will be consumed by the driver when retrieving results.</p>
     *
     * <p>When read-ahead is set to any number greater than 0, a background thread will be started to perform retrieval.
     * To supply an {@link ExecutorService} for the threads, see {@link #readAhead(int, ExecutorService)}.</p>
     *
     * <p>When the executor is not provided, a new {@link Thread} is created to perform the retrieval.</p>
     *
     * <p>
     *     The readAhead is optional.
     * </p>
     *
     * @param readAhead
     *              The number of read-ahead buffers for each open result set, 0 for no asynchronous read-ahead.
     *
     * @return This builder object.
     */
    QldbDriverBuilder readAhead(int readAhead);

    /**
     * <p>Specify the number of read-ahead buffers, determining the amount of sets of results buffered in memory,
     * for each open result set, created within the driver.</p>
     *
     * <p>The higher the read-ahead buffer count, the more memory will be consumed by the driver when retrieving results.</p>
     *
     * <p>When read-ahead is set to any number greater than 0, the supplied {@link ExecutorService} will be used to
     * asynchronously retrieved results. To simply start new threads, see {@link #readAhead(int)}.</p>
     *
     * <p>
     *     The readAhead and executorService are optional.
     * </p>
     *
     * @param readAhead
     *              The number of read-ahead buffers for each open result set, 0 for no asynchronous read-ahead.
     * @param executorService
     *              The executor to be used by the retrieval thread.
     *
     * @return This builder object.
     */
    QldbDriverBuilder readAhead(int readAhead, ExecutorService executorService);

    /**
     * <p>Specify the http client builder that should be used for making http requests.</p>
     *
     * <p>Note that if maximum connections is set in the http client, it should be equal or greater than
     * {@link #maxConcurrentTransactions(int)} to avoid connection contentions and poor performance.
     * If no parameter is specified then a default value would be used.</p>
     *
     * <p>
     *     The httpClientBuilder is optional.
     * </p>
     *
     * @param httpClientBuilder
     *              The builder used to create the http client.
     *
     * @return This builder object.
     */
    QldbDriverBuilder httpClientBuilder(SdkAsyncHttpClient.Builder httpClientBuilder);
}
