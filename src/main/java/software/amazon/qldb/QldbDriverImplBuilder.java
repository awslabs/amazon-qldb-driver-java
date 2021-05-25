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
import com.amazon.ion.system.IonSystemBuilder;
import java.util.MissingResourceException;
import java.util.ResourceBundle;
import java.util.concurrent.ExecutorService;
import software.amazon.awssdk.core.client.config.SdkAdvancedClientOption;
import software.amazon.awssdk.core.internal.http.loader.DefaultSdkHttpClientBuilder;
import software.amazon.awssdk.http.SdkHttpConfigurationOption;
import software.amazon.awssdk.services.qldbsession.QldbSessionClientBuilder;
import software.amazon.awssdk.utils.AttributeMap;
import software.amazon.awssdk.utils.Validate;

/**
 * Builder object for creating a {@link QldbDriver}, allowing for configuration of the parameters of
 * construction.
 */
class QldbDriverImplBuilder implements QldbDriverBuilder {
    private static final String VERSION = "QLDB Driver for Java v";
    private static final String VERSION_KEY = "project.version";

    private static final IonSystem DEFAULT_ION_SYSTEM = IonSystemBuilder.standard().build();
    private static final int DEFAULT_READAHEAD = 0;
    private static final int DEFAULT_MAX_CONCURRENT_TRANSACTIONS = SdkHttpConfigurationOption.GLOBAL_HTTP_DEFAULTS
            .get(SdkHttpConfigurationOption.MAX_CONNECTIONS);

    private int maxConcurrentTransactions = DEFAULT_MAX_CONCURRENT_TRANSACTIONS;
    private int readAhead = DEFAULT_READAHEAD;
    private ExecutorService executorService;
    private QldbSessionClientBuilder clientBuilder;
    private String ledgerName;
    private RetryPolicy retryPolicy = RetryPolicy.builder().build();
    private IonSystem ionSystem = DEFAULT_ION_SYSTEM;

    QldbDriverImplBuilder() {
    }

    @Override
    public QldbDriver build() {
        Validate.paramNotBlank(ledgerName, "ledgerName");
        Validate.paramNotNull(clientBuilder, "client");
        return createDriver();
    }

    @Override
    public QldbDriverBuilder ionSystem(IonSystem ionSystem) {
        Validate.paramNotNull(ionSystem, "ionSystem");
        this.ionSystem = ionSystem;
        return this;
    }

    @Override
    public QldbDriverBuilder ledger(String ledgerName) {
        Validate.paramNotBlank(ledgerName, "ledgerName");
        this.ledgerName = ledgerName;
        return this;
    }

    @Override
    public QldbDriverBuilder transactionRetryPolicy(RetryPolicy retryPolicy) {
        Validate.notNull(retryPolicy, "retryPolicy");
        this.retryPolicy = retryPolicy;
        return this;
    }

    @Override
    public QldbDriverBuilder sessionClientBuilder(QldbSessionClientBuilder clientBuilder) {
        Validate.paramNotNull(clientBuilder, "clientBuilder");
        this.clientBuilder = clientBuilder;
        return this;
    }

    /**
     * <p>Specify the limit to the pool of available sessions.</p>
     *
     * <p>Attempting to retrieve a session when the maximum number of sessions is already withdrawn will block until
     * a session becomes available.</p>
     *
     * @param maxConcurrentTransactions
     *              The maximum number of sessions that can be created from the pool at any one time.
     *
     * @return This builder object.
     */
    @Override
    public QldbDriverBuilder maxConcurrentTransactions(int maxConcurrentTransactions) {
        Validate.isPositive(maxConcurrentTransactions, "maxConcurrentTransactions");
        this.maxConcurrentTransactions = maxConcurrentTransactions;
        return this;
    }

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
     * @param readAhead
     *              The number of read-ahead buffers for each open result set, 0 for no asynchronous read-ahead.
     *
     * @return This builder object.
     */
    @Override
    public QldbDriverBuilder readAhead(int readAhead) {
        software.amazon.qldb.Validate.assertIsAtLeastTwo(readAhead, "readAhead");
        this.readAhead = readAhead;
        this.executorService = null;
        return this;
    }

    /**
     * <p>Specify the number of read-ahead buffers, determining the amount of sets of results buffered in memory,
     * for each open result set, created within the driver.</p>
     *
     * <p>The higher the read-ahead buffer count, the more memory will be consumed by the driver when retrieving results.</p>
     *
     * <p>When read-ahead is set to any number greater than 0, the supplied {@link ExecutorService} will be used to
     * asynchronously retrieved results. To simply start new threads, see {@link #readAhead(int)}.</p>
     *
     * @param readAhead
     *              The number of read-ahead buffers for each open result set, 0 for no asynchronous read-ahead.
     * @param executorService
     *              The executor to be used by the retrieval thread.
     *
     * @return This builder object.
     */
    @Override
    public QldbDriverBuilder readAhead(int readAhead, ExecutorService executorService) {
        Validate.isNotNegative(readAhead, "readAhead");
        Validate.notNull(executorService, "executorService");
        this.readAhead = readAhead;
        this.executorService = executorService;
        return this;
    }

    private QldbDriver createDriver() {
        clientBuilder.applyMutation(client -> {
            client.overrideConfiguration(oc -> {
                oc.putAdvancedOption(SdkAdvancedClientOption.USER_AGENT_PREFIX, getVersion());
                oc.retryPolicy(software.amazon.awssdk.core.retry.RetryPolicy.builder().numRetries(0).build());
            });
        });
        if (maxConcurrentTransactions != DEFAULT_MAX_CONCURRENT_TRANSACTIONS) {
            AttributeMap httpConfig = AttributeMap
                    .builder()
                    .put(SdkHttpConfigurationOption.MAX_CONNECTIONS, maxConcurrentTransactions)
                    .build();

            clientBuilder.httpClient(new DefaultSdkHttpClientBuilder().buildWithDefaults(httpConfig));
        }
        return new QldbDriverImpl(ledgerName, clientBuilder.build(), retryPolicy, readAhead, maxConcurrentTransactions, ionSystem,
                                  executorService);
    }

    private String getVersion() {
        String version;
        try {
            version = VERSION + ResourceBundle.getBundle("version").getString(VERSION_KEY);
        } catch (MissingResourceException e) {
            version = VERSION + "unknown";
        }
        return version;
    }
}
