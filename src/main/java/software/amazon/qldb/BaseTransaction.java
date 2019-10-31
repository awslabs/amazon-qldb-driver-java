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

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import com.amazon.ion.IonSystem;
import com.amazon.ion.IonValue;
import com.amazonaws.util.ValidationUtils;

/**
 * The abstract base transaction, containing the properties and methods shared by the asynchronous and synchronous implementations
 * of a QLDB transaction.
 */
abstract class BaseTransaction implements AutoCloseable {
    final BaseQldbSession qldbSession;
    final Session session;
    final String txnId;
    final AtomicBoolean isClosed = new AtomicBoolean(true);
    private QldbHash txnHash;
    final IonSystem ionSystem;

    BaseTransaction(BaseQldbSession qldbSession, String txnId, IonSystem ionSystem) {
        ValidationUtils.assertNotNull(qldbSession, "qldbSession");
        ValidationUtils.assertNotNull(txnId, "txnId");

        this.qldbSession = qldbSession;
        this.session = qldbSession.session;
        this.txnId = txnId;
        this.txnHash = QldbHash.toQldbHash(this.txnId, ionSystem);
        this.ionSystem = ionSystem;
        this.isClosed.set(false);
    }

    public String getTransactionId() {
        return txnId;
    }

    /**
     * Get this transaction's commit digest hash.
     *
     * @return The current commit digest hash.
     */
    QldbHash getTransactionHash() {
        return txnHash;
    }

    /**
     * Update this transaction's commit digest hash.
     *
     * @param hash
     *              The new commit digest hash to replace the old one.
     */
    void setTransactionHash(QldbHash hash) {
        txnHash = hash;
    }

    /**
     * Apply the dot function on a seed {@link QldbHash} given a statement and parameters.
     *
     * @param seed
     *              The current QldbHash representing the transaction's current commit digest.
     * @param statement
     *              The PartiQL statement to be executed against QLDB.
     * @param parameters
     *              The parameters to be used with the PartiQL statement, for each ? placeholder in the statement.
     *
     * @return The new QldbHash for the transaction.
     */
    static QldbHash dot(QldbHash seed, String statement, List<IonValue> parameters, IonSystem ionSystem) {
        QldbHash statementHash = QldbHash.toQldbHash(statement, ionSystem);
        for (IonValue param : parameters) {
            statementHash = statementHash.dot(QldbHash.toQldbHash(param, ionSystem));
        }
        return seed.dot(statementHash);
    }
}
