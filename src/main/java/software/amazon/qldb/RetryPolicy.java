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

import software.amazon.awssdk.utils.Validate;

/**
 * RetryPolicy used to retry the transactions.
 *
 * <p>
 *     <b>Note</b>: If no retry policy is specified when creating the {@link QldbDriver} then a
 *     Retry Policy created will be created with the default parameters.
 * </p>
 *
 * The default parameters are:
 * <ul>
 *     <li>Retry attempts: 4</li>
 *     <li>BackoffStrategy: {@link DefaultQldbTransactionBackoffStrategy} </li>
 * </ul>
 *
 * <p>
 * The builder can be customized with the maximum number of retry attempts and the backoff strategy, used to compute the delay
 * before the next retry request. Example:
 * </p>
 *
 * <pre>{@code
 * RetryPolicy retryPolicy = RetryPolicy.builder().maxRetries(3).build()
 *
 * QldbDriver QldbDriver = QldbDriver.builder()
 *                 .sessionClientBuilder(mockBuilder)
 *                 .ledger(LEDGER)
 *                 .transactionRetryPolicy(retryPolicy)
 *                 .maxConcurrentTransactions(TXN_LIMIT)
 *                 .build();
 * }</pre>
 *
 */
public final class RetryPolicy {

    private int maxRetries;

    private BackoffStrategy backoffStrategy;

    RetryPolicy(BackoffStrategy backoffStrategy, int maxRetries) {
        Validate.notNull(backoffStrategy, "backoffStrategy");
        this.backoffStrategy = backoffStrategy;
        this.maxRetries = maxRetries;
    }

    int maxRetries() {
        return maxRetries;
    }

    BackoffStrategy backoffStrategy() {
        return backoffStrategy;
    }

    /**
     * Factory method to create a RetryPolicy that will not retry.
     *
     * @return A retry policy instance
     */
    public static RetryPolicy none() {
        return RetryPolicy.builder()
                          .maxRetries(0)
                          .backoffStrategy(new DefaultQldbTransactionBackoffStrategy())
                          .build();
    }

    /**
     * <p>Factory method to create a RetryPolicy with the maximum retry number that a transaction will be executed.</p>
     *
     * <b>Note:</b> The {@link DefaultQldbTransactionBackoffStrategy} backoff strategy will be used when creating the
     * RetryPolicy with this method.
     *
     * @param maxRetries Maximum number of times that the transaction will be retried.
     *
     * @return A retry policy instance
     */
    public static RetryPolicy maxRetries(int maxRetries) {
        return RetryPolicy.builder()
                          .maxRetries(maxRetries)
                          .backoffStrategy(new DefaultQldbTransactionBackoffStrategy())
                          .build();
    }

    /**
     * Creates a builder instance of the RetryPolicy.
     *
     * @return Builder with recommended defaults.
     */
    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        public static final int DEFAULT_RETRY_LIMIT = 4;

        private BackoffStrategy backoffStrategy = new DefaultQldbTransactionBackoffStrategy();

        private int maxRetries = DEFAULT_RETRY_LIMIT;

        private Builder() {
        }

        /**
         *
         * The default value for max number of retries is {@value Builder#DEFAULT_RETRY_LIMIT}.
         *
         * @param maxRetries Max number of retries to allow.
         * @return This object for method chaining.
         */
        public Builder maxRetries(int maxRetries) {
            this.maxRetries = maxRetries;
            return this;
        }

        /**
         *
         * <p>If backoff strategy is not provided, {@link DefaultQldbTransactionBackoffStrategy} will be used.</p>
         *
         * <p>See {@link software.amazon.qldb.DefaultQldbTransactionBackoffStrategy} for a preconfigured implementation.</p>
         *
         * @param backoffStrategy Backoff strategy to use in the RetryPolicy.
         * @return This object for method chaining.
         */
        public Builder backoffStrategy(BackoffStrategy backoffStrategy) {
            Validate.notNull(backoffStrategy, "backoffStrategy");
            this.backoffStrategy = backoffStrategy;
            return this;
        }

        /**
         * Construct a RetryPolicy with the current configuration in the builder.
         *
         * @return Configured RetryPolicy object.
         */
        public RetryPolicy build() {
            return new RetryPolicy(backoffStrategy, maxRetries);
        }
    }
}