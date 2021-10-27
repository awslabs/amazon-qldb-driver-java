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

import io.reactivex.rxjava3.core.BackpressureStrategy;
import io.reactivex.rxjava3.subjects.PublishSubject;
import org.reactivestreams.Publisher;
import software.amazon.awssdk.core.async.SdkPublisher;
import software.amazon.awssdk.services.qldbsessionv2.QldbSessionV2AsyncClient;
import software.amazon.awssdk.services.qldbsessionv2.model.CommandStream;
import software.amazon.awssdk.services.qldbsessionv2.model.ResultStream;
import software.amazon.awssdk.services.qldbsessionv2.model.SendCommandRequest;
import software.amazon.awssdk.services.qldbsessionv2.model.SendCommandResponse;
import software.amazon.awssdk.services.qldbsessionv2.model.SendCommandResponseHandler;
import software.amazon.awssdk.utils.CompletableFutureUtils;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

public class MockQldbSessionClient implements QldbSessionV2AsyncClient {

    private final PublishSubject<ResultStream> resultToDeliver;
    private final Queue<Holder> resultQueue;

    private static class Holder {
        public final SendCommandResponse response;
        public final Exception exception;
        public final ResultStream result;

        public Holder(SendCommandResponse response) {
            this.response = response;
            this.exception = null;
            this.result = null;
        }

        public Holder(Exception e) {
            this.response = null;
            this.exception = e;
            this.result = null;
        }

        public Holder(ResultStream result) {
            this.response = null;
            this.exception = null;
            this.result = result;
        }
    }

    public MockQldbSessionClient() {
        resultQueue = new ArrayDeque<>();
        resultToDeliver = PublishSubject.create();
    }

    @Override
    public CompletableFuture<Void> sendCommand(SendCommandRequest sendCommandRequest, Publisher<CommandStream> requestStream, SendCommandResponseHandler asyncResponseHandler) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        CompletableFuture<Void> executeFuture = CompletableFuture.runAsync(() -> {
            while (resultQueue.peek() != null) {
                Holder holder = resultQueue.remove();
                if (holder.exception != null) {
                    try {
                        Thread.sleep(3000L);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    throw new CompletionException(holder.exception);
                } else if (holder.response != null) {
                    asyncResponseHandler.responseReceived(holder.response);
                    final SdkPublisher<ResultStream> sdkPublisher = SdkPublisher.adapt(resultToDeliver.toFlowable(BackpressureStrategy.ERROR));
                    asyncResponseHandler.onEventStream(sdkPublisher);
                } else if (holder.result != null) {
                    resultToDeliver.onNext(holder.result);
                }
            }
        });

        CompletableFuture<Void> whenCompleted = executeFuture.whenComplete((r, e) -> {
            if (e != null) {
                try {
                    asyncResponseHandler.exceptionOccurred(e);
                } finally {
                    future.completeExceptionally(e);
                }
            }
        });
        executeFuture = CompletableFutureUtils.forwardExceptionTo(whenCompleted, executeFuture);
        return CompletableFutureUtils.forwardExceptionTo(future, executeFuture);

    }

    public boolean isQueueEmpty() {
        return resultQueue.isEmpty();
    }

    public void queueResponse(Exception e) {
        resultQueue.add(new Holder(e));
    }

    public MockQldbSessionClient queueResponse(SendCommandResponse response) {
        resultQueue.add(new Holder(response));
        return this;
    }

    public MockQldbSessionClient queueResponse(ResultStream result) {
        resultQueue.add(new Holder(result));
        return this;
    }

    public MockQldbSessionClient startDummySession() {
        queueResponse(MockResponses.SEND_COMMAND_RESPONSE);
        return this;
    }

    public MockQldbSessionClient addEndSession() {
        return queueResponse(MockResponses.END_SESSION_RESULT);
    }

    public MockQldbSessionClient addDummyTransaction() throws IOException {
        return addDummyTransaction(null);
    }

    public MockQldbSessionClient addDummyTransaction(String txnId) throws IOException {
        txnId = txnId == null ? "id" : txnId;
        queueResponse(MockResponses.startTxnResponse(txnId));
        queueResponse(MockResponses.executeResponse(Collections.emptyList()));
        queueResponse(MockResponses.commitTransactionResponse(txnId));
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
