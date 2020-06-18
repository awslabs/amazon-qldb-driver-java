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

import java.time.Duration;
import java.util.Random;
import software.amazon.awssdk.utils.Validate;

/**
 * BackoffStrategy that calculates the time to delay the execution of the next transaction.
 *
 * <p>
 * The backoff strategy uses a min delay of 10ms and max delay of 5000ms using an
 * equal jitter strategy.
 * </p>
 *
 * <pre>{@code
 * exponential delay  = min( base delay * (2 ^ retry attempts), max delay)
 * jitter = random()
 * sleep time = exponential delay / 2 + jitter * exponential delay / 2
 *
 * }</pre>
 */
public class DefaultQldbTransactionBackoffStrategy implements BackoffStrategy {
    static final Duration BASE_DELAY_CEILING = Duration.ofMillis(10);
    private static final Duration MAX_BACKOFF_CEILING = Duration.ofMillis(5000);
    /**
     * Max permitted retry times. To prevent exponentialDelay from overflow, there must be 2 ^ retriesAttempted
     * <= 2 ^ 31 - 1, which means retriesAttempted <= 30, so that is the ceil for retriesAttempted.
     */
    private static final int RETRIES_ATTEMPTED_CEILING = (int) Math.floor(Math.log(Integer.MAX_VALUE) / Math.log(2));

    private final Duration baseDelay ;
    private final Duration maxBackoffTime;
    private final Random random;


    DefaultQldbTransactionBackoffStrategy() {
        this(BASE_DELAY_CEILING, MAX_BACKOFF_CEILING, new Random());
    }

    DefaultQldbTransactionBackoffStrategy(final Duration baseDelay, final Duration maxBackoffTime, final Random random) {
        this.baseDelay = Validate.isNotNegative(baseDelay, "baseDelay");
        this.maxBackoffTime = Validate.isNotNegative(maxBackoffTime, "maxBackoffTime");

        if (baseDelay.compareTo(maxBackoffTime) > 0) {
            throw new IllegalArgumentException(String.format("%s cannot be greater than %s", "baseDelay", "maxBackoffTime"));
        }

        this.random = random;
    }

    /**
     * Constructor to create a Backoff Strategy but with custom baseDelay and cap backoff time.
     * @param baseDelay The minimum amount of time to delay a retry.
     * @param maxBackoffTime The maximum time to delay a retry.
     */
    public DefaultQldbTransactionBackoffStrategy(final Duration baseDelay, final Duration maxBackoffTime) {
        this(baseDelay, maxBackoffTime, new Random());
    }

    /**
     * <p>Compute the delay before the next retry request.</p>
     *
     * <p>This strategy is only consulted when there will be a next retry.</p>
     *
     * @param retryPolicyContext Number of retries that will be attempted.
     * @return Amount of time in milliseconds to wait before the next attempt. Must be non-negative (can be zero).
     */
    @Override
    public Duration calculateDelay(RetryPolicyContext retryPolicyContext) {
        int cappedRetries = Math.min(retryPolicyContext.retriesAttempted(), RETRIES_ATTEMPTED_CEILING);
        int delay = (int) Math.min(baseDelay.multipliedBy(1L << cappedRetries).toMillis(), maxBackoffTime.toMillis());
        return Duration.ofMillis((delay / 2) + random.nextInt((delay / 2) + 1));
    }

}
