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
import com.amazon.ion.system.IonSystemBuilder;
import com.amazonaws.ClientConfigurationFactory;
import com.amazonaws.services.qldbsession.AmazonQLDBSession;
import com.amazonaws.services.qldbsession.AmazonQLDBSessionClientBuilder;
import com.amazonaws.util.ValidationUtils;
import java.util.MissingResourceException;
import java.util.ResourceBundle;

// CHECKSTYLE:OFF: ClassTypeParameterName - Using words instead capitalized characters for templates.
abstract class BaseQldbDriverBuilder<Subclass extends BaseQldbDriverBuilder, TypeToBuild> {
    // CHECKSTYLE:ON: ClassTypeParameterName
    private static final String VERSION_KEY = "project.version";
    private static final int DEFAULT_RETRY_LIMIT = 4;
    private static final IonSystem DEFAULT_ION_SYSTEM = IonSystemBuilder.standard().build();

    int clientMaxConnections;
    AmazonQLDBSession client;
    String ledgerName;
    int retryLimit = DEFAULT_RETRY_LIMIT;
    IonSystem ionSystem = DEFAULT_ION_SYSTEM;

    /**
     * Build a {@link TypeToBuild} instance using the current configuration set with the builder.
     *
     * @return A newly created {@link TypeToBuild}.
     */
    public TypeToBuild build() {
        ValidationUtils.assertStringNotEmpty(ledgerName, "ledgerName");
        ValidationUtils.assertNotNull(client, "client");
        return createDriver();
    }

    /**
     * Specify the {@link IonSystem} that should be used for the driver's sessions.
     *
     * @param ionSystem
     *              The {@link IonSystem} to use.
     *
     * @return This builder object.
     */
    public Subclass withIonSystem(IonSystem ionSystem) {
        ValidationUtils.assertNotNull(ionSystem, "ionSystem");
        this.ionSystem = ionSystem;
        return getSubclass();
    }

    /**
     * Specify the ledger that should be used for the driver's sessions.
     *
     * @param ledgerName
     *              The name of the ledger to create a driver with.
     *
     * @return This builder object.
     */
    public Subclass withLedger(String ledgerName) {
        ValidationUtils.assertStringNotEmpty(ledgerName, "ledgerName");
        this.ledgerName = ledgerName;
        return getSubclass();
    }

    /**
     * Specify the retry limit that any convenience execute methods provided by sessions created from the driver will
     * attempt.
     *
     * @param retryLimit
     *              The number of retry attempts to be made by the session.
     *
     * @return This builder object.
     */
    public Subclass withRetryLimit(int retryLimit) {
        Validate.assertIsNotNegative(retryLimit, "retryLimit");
        this.retryLimit = retryLimit;
        return getSubclass();
    }

    /**
     * Specify the low level session builder that should be used for creating the low level session used for
     * communication in the driver.
     *
     * Note that the user agent suffix and retry count will be set on the passed in session builder.
     *
     * @param clientBuilder
     *              The builder used to create the low-level session.
     *
     * @return This builder object.
     */
    public Subclass withSessionClientBuilder(AmazonQLDBSessionClientBuilder clientBuilder) {
        ValidationUtils.assertNotNull(clientBuilder, "clientBuilder");

        String version;
        try {
            version = getVersion() + ResourceBundle.getBundle("version").getString(VERSION_KEY);
        } catch (MissingResourceException e) {
            version = getVersion() + "unknown";
        }

        if (null == clientBuilder.getClientConfiguration()) {
            clientBuilder.setClientConfiguration(new ClientConfigurationFactory().getConfig());
        }

        clientBuilder.getClientConfiguration().setUserAgentSuffix(version);
        clientBuilder.getClientConfiguration().setMaxErrorRetry(0);
        this.clientMaxConnections = clientBuilder.getClientConfiguration().getMaxConnections();
        this.client = clientBuilder.build();
        return getSubclass();
    }

    /**
     * Create a new driver instance using the current configuration.
     *
     * @return The newly created driver instance.
     */
    protected abstract TypeToBuild createDriver();

    /**
     * Helper method to returned a type cast version of this builder as the subclass type.
     *
     * @return The subclass cast version of this builder.
     */
    protected Subclass getSubclass() {
        return (Subclass) this;
    }

    /**
     * Retrieve the version string for the driver.
     *
     * @return The version string for the driver.
     */
    protected abstract String getVersion();
}