/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package software.amazon.qldbstreaming.integrationtests;

import java.net.URI;
import java.util.logging.Level;
import java.util.logging.Logger;
import software.amazon.awssdk.core.internal.http.loader.DefaultSdkAsyncHttpClientBuilder;
import software.amazon.awssdk.http.Protocol;
import software.amazon.awssdk.http.SdkHttpConfigurationOption;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.qldb.QldbClient;
import software.amazon.awssdk.services.qldb.model.CreateLedgerRequest;
import software.amazon.awssdk.services.qldb.model.DeleteLedgerRequest;
import software.amazon.awssdk.services.qldb.model.DescribeLedgerRequest;
import software.amazon.awssdk.services.qldb.model.DescribeLedgerResponse;
import software.amazon.awssdk.services.qldb.model.LedgerState;
import software.amazon.awssdk.services.qldb.model.PermissionsMode;
import software.amazon.awssdk.services.qldb.model.ResourceAlreadyExistsException;
import software.amazon.awssdk.services.qldb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.qldb.model.UpdateLedgerRequest;
import software.amazon.awssdk.services.qldbsessionv2.QldbSessionV2AsyncClient;
import software.amazon.awssdk.services.qldbsessionv2.QldbSessionV2AsyncClientBuilder;
import software.amazon.awssdk.utils.AttributeMap;
import software.amazon.qldbstreaming.QldbDriver;
import software.amazon.qldbstreaming.QldbDriverBuilder;
import software.amazon.qldbstreaming.RetryPolicy;

public class LedgerManager {
    private static final Logger logger = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
    private final String ledgerName;
    private final String regionName;
    private final QldbClient client;

    public LedgerManager(final String ledger, final String region) {
        this.ledgerName = ledger;
        this.regionName = region;
        this.client = QldbClient.builder().region(Region.of(regionName))
            .endpointOverride(URI.create("https://frontend-547110709870.dev.qldb.aws.a2z.com")).build();
    }

    public QldbDriver createQldbDriver(final String ledger, final int poolLimit, final int retryLimit) {
        QldbDriverBuilder builder = QldbDriver.builder().ledger(ledger);
        if (poolLimit != Constants.DEFAULT) {
            builder.maxConcurrentTransactions(poolLimit);
        }
        if (retryLimit != Constants.DEFAULT) {
            builder.transactionRetryPolicy(RetryPolicy.builder().maxRetries(retryLimit).build());
        } else {
            builder.transactionRetryPolicy(RetryPolicy.builder().maxRetries(retryLimit).build());
        }

        QldbSessionV2AsyncClientBuilder sessionClientBuilder = QldbSessionV2AsyncClient.builder()
            .region(Region.of(regionName))
            .endpointOverride(URI.create("https://session-547110709870.dev.qldb.aws.a2z.com"));

        AttributeMap httpConfig = AttributeMap
            .builder()
            .put(SdkHttpConfigurationOption.PROTOCOL, Protocol.HTTP2)
            .put(SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES, true)
            .build();
        builder.sessionClientBuilder(sessionClientBuilder);
        builder.httpClientBuilder(attributeMap -> new DefaultSdkAsyncHttpClientBuilder().buildWithDefaults(httpConfig));

        return builder.build();
    }

    public QldbDriver createQldbDriver(final int poolLimit, final int retryLimit) {
        return createQldbDriver(this.ledgerName, poolLimit, retryLimit);
    }

    private DescribeLedgerResponse describeLedger(final String ledger) {
        DescribeLedgerRequest request = DescribeLedgerRequest.builder().name(ledger).build();
        return this.client.describeLedger(request);
    }

    private void createLedger() throws InterruptedException {
        logger.log(Level.INFO, "Creating ledger");

        CreateLedgerRequest request = CreateLedgerRequest.builder()
            .name(this.ledgerName)
            .permissionsMode(PermissionsMode.ALLOW_ALL)
            .build();
        this.client.createLedger(request);

        waitForActive(this.ledgerName);

        logger.log(Level.INFO, String.format("'%s' ledger created successfully.", this.ledgerName));
    }

    private void waitForActive(final String ledger) throws InterruptedException {
        logger.log(Level.INFO, "Waiting for ledger to become active...");

        while (true) {
            DescribeLedgerResponse result = describeLedger(ledger);
            if (result.state().equals(LedgerState.ACTIVE)) {
                logger.log(Level.INFO, "Success. Ledger is active and ready to use.");
                return;
            }
            logger.log(Level.INFO, "The ledger is still creating, current state {}. Please wait...", result.state());
            Thread.sleep(Constants.LEDGER_POLL_PERIOD_MS);
        }
    }

    public void deleteLedger() throws InterruptedException {
        logger.log(Level.INFO, "Deleting ledger");

        updateLedgerDeletionProtection(this.ledgerName, false);

        DeleteLedgerRequest request = DeleteLedgerRequest.builder().name(this.ledgerName).build();
        this.client.deleteLedger(request);

        waitForDeletion(this.ledgerName);
    }

    private void updateLedgerDeletionProtection(final String ledger, final boolean deletionProtection) {
        UpdateLedgerRequest request = UpdateLedgerRequest.builder()
            .name(ledger)
            .deletionProtection(deletionProtection)
            .build();

        this.client.updateLedger(request);
    }

    private void waitForDeletion(final String ledger) throws InterruptedException {
        DescribeLedgerRequest ledgerRequest = DescribeLedgerRequest.builder().name(ledger).build();

        while (true) {
            try {
                this.client.describeLedger(ledgerRequest);
                Thread.sleep(Constants.LEDGER_POLL_PERIOD_MS);
            } catch (ResourceNotFoundException e) {
                logger.log(Level.INFO, String.format("'%s' ledger deleted successfully.", ledger));
                break;
            }
        }
    }

    public void runCreateLedger() throws InterruptedException {
        try {
            createLedger();
        } catch (ResourceAlreadyExistsException e) {
            logger.log(Level.INFO, "Ledger already exists. Deleting and creating again.");
            deleteLedger();
            createLedger();
        }
    }
}
