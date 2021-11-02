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

package software.amazon.qldbstreaming;

import com.amazon.ion.IonValue;
import com.amazon.ion.IonWriter;
import com.amazon.ion.system.IonBinaryWriterBuilder;
import io.reactivex.rxjava3.core.BackpressureStrategy;
import io.reactivex.rxjava3.subjects.PublishSubject;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.annotations.NotThreadSafe;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.qldbsessionv2.QldbSessionV2AsyncClient;
import software.amazon.awssdk.services.qldbsessionv2.model.AbortTransactionRequest;
import software.amazon.awssdk.services.qldbsessionv2.model.CommandStream;
import software.amazon.awssdk.services.qldbsessionv2.model.CommitTransactionRequest;
import software.amazon.awssdk.services.qldbsessionv2.model.EndSessionRequest;
import software.amazon.awssdk.services.qldbsessionv2.model.ExecuteStatementRequest;
import software.amazon.awssdk.services.qldbsessionv2.model.FetchPageRequest;
import software.amazon.awssdk.services.qldbsessionv2.model.ResultStream;
import software.amazon.awssdk.services.qldbsessionv2.model.SendCommandRequest;
import software.amazon.awssdk.services.qldbsessionv2.model.SendCommandResponse;
import software.amazon.awssdk.services.qldbsessionv2.model.SendCommandResponseHandler;
import software.amazon.awssdk.services.qldbsessionv2.model.StartTransactionRequest;
import software.amazon.awssdk.services.qldbsessionv2.model.ValueHolder;
import software.amazon.qldbstreaming.exceptions.Errors;
import software.amazon.qldbstreaming.exceptions.QldbDriverException;

/**
 * Session object representing a communication channel with QLDB.
 * <p>
 * This object is not thread-safe.
 */
@NotThreadSafe
class Session {
    private static final Logger logger = LoggerFactory.getLogger(Session.class);
    private final PublishSubject<CommandStream> commandStreamPublishSubject;
    private final LinkedBlockingQueue<CompletableFuture<ResultStream>> futures;
    private String sessionId;

    private Session(LinkedBlockingQueue<CompletableFuture<ResultStream>> futures,
                    PublishSubject<CommandStream> commandStreamPublishSubject,
                    String sessionId) {
        this.futures = futures;
        this.commandStreamPublishSubject = commandStreamPublishSubject;
        this.sessionId = sessionId;
    }

    private void sendCommand(CommandStream command) {
        logger.info("Sending request: {}", command);
        commandStreamPublishSubject.onNext(command);
    }

    static Session startSession(String ledgerName, QldbSessionV2AsyncClient client)
            throws ExecutionException, InterruptedException {
        // Assume 1 request-response may be inflight at any given time. Every time driver sends a command,
        // we queue a future and wait until the future is completed successfully or exceptionally.
        // TODO: Increase queue capacity for pipelining requests.
        final LinkedBlockingQueue<CompletableFuture<ResultStream>> futures = new LinkedBlockingQueue<>(1);
        final ResultStreamSubscriber resultStreamSubscriber = new ResultStreamSubscriber(futures);
        PublishSubject<CommandStream> commandStreamPublishSubject = PublishSubject.create();
        final CompletableFuture<SendCommandResponse> connectionFuture = new CompletableFuture<>();

        final SendCommandRequest sendCommandRequest = SendCommandRequest.builder().ledgerName(ledgerName).build();
        logger.info("Sending SendCommand request: {}", sendCommandRequest);

        // TODO: Revisit back pressure strategy.
        client.sendCommand(sendCommandRequest, commandStreamPublishSubject.toFlowable(BackpressureStrategy.ERROR),
            getResponseHandler(connectionFuture, commandStreamPublishSubject, futures, resultStreamSubscriber));
        SendCommandResponse response = connectionFuture.get();
        return new Session(futures, commandStreamPublishSubject, response.sessionId());
    }

    private static SendCommandResponseHandler getResponseHandler(
        CompletableFuture<SendCommandResponse> connectionFuture,
        PublishSubject<CommandStream> commandStreamPublishSubject,
        LinkedBlockingQueue<CompletableFuture<ResultStream>> futures,
        ResultStreamSubscriber resultStreamSubscriber
    ) {
        return SendCommandResponseHandler.builder()
            .onResponse(sendCommandResponse -> {
                logger.info("Response handler received inital response {}", sendCommandResponse);
                connectionFuture.complete(sendCommandResponse);
            })
            .onError(e -> {
                logger.info("An error occurred while establishing the connection or streaming the response: {}", e.getMessage(),
                    e);
                if (!connectionFuture.isDone()) {
                    connectionFuture.completeExceptionally(e);
                } else {
                    final CompletableFuture<ResultStream> future;
                    try {
                        future = futures.take();
                        future.completeExceptionally(e);
                        commandStreamPublishSubject.onComplete();
                        resultStreamSubscriber.onError(e);
                    } catch (InterruptedException ex) {
                        logger.warn("Errors completing future: {}", ex.getMessage(), ex);
                    }
                }
            })
            .onEventStream(publisher -> {
                logger.info("Events are ready to be streamed to event resultStreamSubscriber");
                publisher.subscribe(resultStreamSubscriber);
            })
            .onComplete(() -> {
                logger.info("All data has been successfully published to event stream resultStreamSubscriber");
                commandStreamPublishSubject.onComplete();
                resultStreamSubscriber.onComplete();
            }).build();
    }

    CompletableFuture<ResultStream> sendStartTransaction() throws InterruptedException {
        final StartTransactionRequest startTransactionRequest = CommandStream.startTransactionBuilder().build();
        final CompletableFuture<ResultStream> future = new CompletableFuture<>();
        futures.put(future);
        sendCommand(startTransactionRequest);
        return future;
    }

    CompletableFuture<ResultStream> sendExecute(String statement, List<IonValue> parameters, String txnId)
            throws InterruptedException {
        final List<ValueHolder> byteParameters = new ArrayList<>(parameters.size());

        if (!parameters.isEmpty()) {
            try {
                final IonBinaryWriterBuilder builder = IonBinaryWriterBuilder.standard();
                final ByteArrayOutputStream stream = new ByteArrayOutputStream();
                final IonWriter writer = builder.build(stream);
                for (IonValue parameter : parameters) {
                    parameter.writeTo(writer);
                    writer.finish();
                    final SdkBytes sdkBytes = SdkBytes.fromByteArray(stream.toByteArray());
                    final ValueHolder value = ValueHolder.builder().ionBinary(sdkBytes).build();
                    byteParameters.add(value);

                    // Reset the stream so that it can be re-used.
                    stream.reset();
                }
            } catch (IOException e) {
                throw QldbDriverException.create(String.format(Errors.SERIALIZING_PARAMS.get(), e.getMessage()),
                    e);
            }
        }
        final ExecuteStatementRequest executeStatementRequest = CommandStream.executeStatementBuilder()
            .statement(statement)
            .parameters(byteParameters)
            .transactionId(txnId)
            .build();
        final CompletableFuture<ResultStream> future = new CompletableFuture<>();
        futures.put(future);
        sendCommand(executeStatementRequest);
        return future;
    }

    CompletableFuture<ResultStream> sendFetchPage(String txnId, String nextPageToken) throws InterruptedException {
        final FetchPageRequest fetchPageRequest = CommandStream.fetchPageBuilder()
            .transactionId(txnId)
            .nextPageToken(nextPageToken)
            .build();
        final CompletableFuture<ResultStream> future = new CompletableFuture<>();
        futures.put(future);
        sendCommand(fetchPageRequest);
        return future;
    }

    CompletableFuture<ResultStream> sendCommit(String txnId) throws InterruptedException {
        final CommitTransactionRequest commitTransactionRequest = CommandStream.commitTransactionBuilder()
            .transactionId(txnId)
            .build();
        final CompletableFuture<ResultStream> future = new CompletableFuture<>();
        futures.put(future);
        sendCommand(commitTransactionRequest);
        return future;
    }

    CompletableFuture<ResultStream> sendAbort() throws InterruptedException {
        final AbortTransactionRequest abortTransactionRequest = CommandStream.abortTransactionBuilder().build();
        final CompletableFuture<ResultStream> future = new CompletableFuture<>();
        futures.put(future);
        sendCommand(abortTransactionRequest);
        return future;
    }

    CompletableFuture<ResultStream> sendEndSession() throws InterruptedException {
        final EndSessionRequest endSessionRequest = CommandStream.endSessionBuilder().build();
        final CompletableFuture<ResultStream> future = new CompletableFuture<>();
        futures.put(future);
        sendCommand(endSessionRequest);
        return future;
    }

    /**
     * Get the session ID.
     *
     * @return This session's ID.
     */
    String getId() {
        return sessionId;
    }

    public void close() {
        try {
            sendEndSession().get();
        } catch (Exception e) {
            // We will only log issues closing the session, as QLDB will clean them up after a timeout.
            logger.warn("Errors closing session: {}", e.getMessage(), e);
        }
    }

}
