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

import com.amazon.ion.IonSystem;

/**
 * The abstract base result, containing the properties and methods shared by the asynchronous and synchronous implementations of
 * the result of executing a statement in QLDB.
 */
abstract class BaseResult {
    final Session session;
    final String txnId;
    final IonSystem ionSystem;

    BaseResult(Session session, String txnId, IonSystem ionSystem) {
        this.session = session;
        this.txnId = txnId;
        this.ionSystem = ionSystem;
    }
}
