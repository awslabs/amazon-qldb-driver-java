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
import com.amazon.ion.IonValue;
import com.amazon.ion.system.IonSystemBuilder;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Queue;
import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.awssdk.services.qldbsession.QldbSessionClient;
import software.amazon.awssdk.services.qldbsession.model.SendCommandRequest;
import software.amazon.awssdk.services.qldbsession.model.SendCommandResponse;

public class MockQldbSessionClient implements QldbSessionClient {
    private static class Holder {

        public final SendCommandResponse result;
        public final RuntimeException exception;

        public Holder(SendCommandResponse result) {
            this.result = result;
            this.exception = null;
        }

        public Holder(RuntimeException e) {
            this.result = null;
            this.exception = e;
        }

    }
    private IonSystem system = IonSystemBuilder.standard().build();

    private final Queue<Holder> resultQueue;

    public MockQldbSessionClient() {
        resultQueue = new ArrayDeque<>();
    }

    @Override
    public SendCommandResponse sendCommand(SendCommandRequest sendCommandRequest) {
        final Holder response = resultQueue.remove();
        if (response.exception != null) {
            throw response.exception;
        }

        return response.result;
    }

    public boolean isQueueEmpty() {
        return resultQueue.isEmpty();
    }

    public MockQldbSessionClient queueResponse(SendCommandResponse response) {
        resultQueue.add(new Holder(response));
        return this;
    }

    public MockQldbSessionClient startDummySession() {
        queueResponse(MockResponses.START_SESSION_RESPONSE);
        return this;
    }

    public MockQldbSessionClient addEndSession() {
        queueResponse(MockResponses.endSessionResponse());
        return this;
    }

    public MockQldbSessionClient addDummyTransaction(String query) throws IOException {
        return addDummyTransaction(query, null);
    }

    public MockQldbSessionClient addDummyTransaction(String query, String txnId) throws IOException {
        txnId = txnId == null ? "id" : txnId;
        QldbHash txnHash = QldbHash.toQldbHash(txnId, system);
        txnHash = Transaction.dot(txnHash, query, Collections.emptyList(), system);
        queueResponse(MockResponses.startTxnResponse(txnId));
        queueResponse(MockResponses.executeResponse(Collections.emptyList()));
        queueResponse(MockResponses.commitTransactionResponse(ByteBuffer.wrap(txnHash.getQldbHash())));
        return this;
    }

    public void queueResponse(RuntimeException e) {
        resultQueue.add(new Holder(e));
    }

    @Override
    public String serviceName() {
        return null;
    }

    @Override
    public void close() {

    }
}
