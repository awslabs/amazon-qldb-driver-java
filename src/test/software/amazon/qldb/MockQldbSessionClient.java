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
import io.reactivex.rxjava3.core.BackpressureStrategy;
import io.reactivex.rxjava3.subjects.PublishSubject;
import org.reactivestreams.Publisher;
import software.amazon.awssdk.core.async.SdkPublisher;
import software.amazon.awssdk.services.qldbsessionv2.QldbSessionV2AsyncClient;
import software.amazon.awssdk.services.qldbsessionv2.model.CommandStream;
import software.amazon.awssdk.services.qldbsessionv2.model.ResultStream;
import software.amazon.awssdk.services.qldbsessionv2.model.SendCommandRequest;
import software.amazon.awssdk.services.qldbsessionv2.model.SendCommandResponseHandler;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

public class MockQldbSessionClient implements QldbSessionV2AsyncClient {
//    public RuntimeException exception = null;
    private final PublishSubject<ResultStream> resultToDeliver;

    public MockQldbSessionClient() {
        this.resultToDeliver = PublishSubject.create();
    }

    @Override
    public CompletableFuture<Void> sendCommand(SendCommandRequest sendCommandRequest, Publisher<CommandStream> requestStream, SendCommandResponseHandler asyncResponseHandler) {
        CompletableFuture<Void> future = new CompletableFuture<>();

//        if (exception != null) {
//            try {
//                asyncResponseHandler.exceptionOccurred(exception);
//            } finally {
//                future.completeExceptionally(exception);
//            }
//        }

        final SdkPublisher<ResultStream> sdkPublisher = SdkPublisher.adapt(resultToDeliver.toFlowable(BackpressureStrategy.ERROR));

        asyncResponseHandler.responseReceived(MockResponses.sendCommandResponse());
        asyncResponseHandler.onEventStream(sdkPublisher);

        return future;
    }

    private IonSystem system = IonSystemBuilder.standard().build();

    public MockQldbSessionClient queueResult(ResultStream result) {
        resultToDeliver.onNext(result);
        return this;
    }

    public MockQldbSessionClient addEndSession() {
        queueResult(MockResponses.endSessionResponse());
        return this;
    }

    public MockQldbSessionClient addDummyTransaction(String query) throws IOException {
        return addDummyTransaction(query, null);
    }

    public MockQldbSessionClient addDummyTransaction(String query, String txnId) throws IOException {
        txnId = txnId == null ? "id" : txnId;
        QldbHash txnHash = QldbHash.toQldbHash(txnId, system);
        txnHash = Transaction.dot(txnHash, query, Collections.emptyList(), system);
        queueResult(MockResponses.startTxnResponse(txnId));
        queueResult(MockResponses.executeResponse(Collections.emptyList()));
        queueResult(MockResponses.commitTransactionResponse(ByteBuffer.wrap(txnHash.getQldbHash())));
        return this;
    }

    @Override
    public String serviceName() {
        return null;
    }

    @Override
    public void close() {

    }
}
