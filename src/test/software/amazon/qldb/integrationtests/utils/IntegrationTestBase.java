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

package software.amazon.qldb.integrationtests.utils;

import com.amazonaws.services.qldb.AmazonQLDB;
import com.amazonaws.services.qldb.AmazonQLDBClientBuilder;
import com.amazonaws.services.qldb.model.CreateLedgerRequest;
import com.amazonaws.services.qldb.model.DeleteLedgerRequest;
import com.amazonaws.services.qldb.model.DescribeLedgerRequest;
import com.amazonaws.services.qldb.model.DescribeLedgerResult;
import com.amazonaws.services.qldb.model.LedgerState;
import com.amazonaws.services.qldb.model.PermissionsMode;
import com.amazonaws.services.qldb.model.ResourceAlreadyExistsException;
import com.amazonaws.services.qldb.model.ResourceNotFoundException;
import com.amazonaws.services.qldb.model.UpdateLedgerRequest;
import com.amazonaws.services.qldbsession.AmazonQLDBSessionClientBuilder;
import java.util.logging.Level;
import java.util.logging.Logger;
import software.amazon.qldb.PooledQldbDriver;

public class IntegrationTestBase {
    private static final Logger logger = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
    private final String ledgerName;
    private final String regionName;
    private final AmazonQLDB client;

    public IntegrationTestBase(final String ledger, final String region) {
        this.ledgerName = ledger;
        this.regionName = region;
        this.client = AmazonQLDBClientBuilder.standard().withRegion(this.regionName).build();
    }

    public PooledQldbDriver createQldbDriver(final String ledger, final int poolLimit, final int timeOut, final int retryLimit) {
        PooledQldbDriver.PooledQldbDriverBuilder builder = PooledQldbDriver.builder().withLedger(ledger);

        if (poolLimit != Constants.DEFAULT) {
            builder.withPoolLimit(poolLimit);
        }

        if (timeOut != Constants.DEFAULT) {
            builder.withPoolTimeout(timeOut);
        }

        if (retryLimit != Constants.DEFAULT) {
            builder.withRetryLimit(retryLimit);
        } else {
            builder.withRetryLimit(Constants.RETRY_LIMIT);
        }

        AmazonQLDBSessionClientBuilder sessionClientBuilder = AmazonQLDBSessionClientBuilder.standard();
        sessionClientBuilder.setRegion(this.regionName);

        builder.withSessionClientBuilder(sessionClientBuilder);

        return builder.build();
    }

    public PooledQldbDriver createQldbDriver(final int poolLimit, final int timeOut, final int retryLimit) {
        return createQldbDriver(this.ledgerName, poolLimit, timeOut, retryLimit);
    }

    private DescribeLedgerResult describeLedger(final String ledger) {
        DescribeLedgerRequest request = new DescribeLedgerRequest().withName(ledger);
        return this.client.describeLedger(request);
    }

    private void createLedger() throws InterruptedException {
        logger.log(Level.INFO, "Creating ledger");

        CreateLedgerRequest request = new CreateLedgerRequest()
            .withName(this.ledgerName)
            .withPermissionsMode(PermissionsMode.ALLOW_ALL);
        this.client.createLedger(request);

        waitForActive(this.ledgerName);

        logger.log(Level.INFO, String.format("'%s' ledger created successfully.", this.ledgerName));
    }

    private void waitForActive(final String ledger) throws InterruptedException {
        logger.log(Level.INFO, "Waiting for ledger to become active...");

        while (true) {
            DescribeLedgerResult result = describeLedger(ledger);
            if (result.getState().equals(LedgerState.ACTIVE.name())) {
                logger.log(Level.INFO, "Success. Ledger is active and ready to use.");
                return;
            }
            logger.log(Level.INFO, "The ledger is still creating. Please wait...");
            Thread.sleep(Constants.LEDGER_POLL_PERIOD_MS);
        }
    }

    public void deleteLedger() throws InterruptedException {
        logger.log(Level.INFO, "Deleting ledger");

        updateLedgerDeletionProtection(this.ledgerName, false);

        DeleteLedgerRequest request = new DeleteLedgerRequest().withName(this.ledgerName);
        this.client.deleteLedger(request);

        waitForDeletion(this.ledgerName);
    }

    private void updateLedgerDeletionProtection(final String ledger, final boolean deletionProtection) {
        UpdateLedgerRequest request = new UpdateLedgerRequest()
            .withName(ledger)
            .withDeletionProtection(deletionProtection);

        this.client.updateLedger(request);
    }

    private void waitForDeletion(final String ledger) throws InterruptedException {
        DescribeLedgerRequest ledgerRequest = new DescribeLedgerRequest().withName(ledger);

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
