package software.amazon.qldb;

import com.amazon.ion.IonValue;
import com.amazon.ion.IonWriter;
import com.amazon.ion.system.IonBinaryWriterBuilder;
import io.reactivex.rxjava3.core.BackpressureStrategy;
import io.reactivex.rxjava3.subjects.PublishSubject;
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
import software.amazon.qldb.exceptions.Errors;
import software.amazon.qldb.exceptions.QldbDriverException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;


/**
 * Session object representing a communication channel with QLDB.
 *
 * This object is not thread-safe.
 */
@NotThreadSafe
class Session {
    private static final Logger logger = LoggerFactory.getLogger(Session.class);
    private final String ledgerName;
    private final QldbSessionV2AsyncClient client;
    private final CompletableFuture<SendCommandResponse> connectionFuture;
    private final PublishSubject<CommandStream> commandStreamSubject;
    private final EventStreamSubscriber eventStreamSubscriber;
    private final LinkedBlockingQueue<CompletableFuture<ResultStream>> futures;
    private String sessionId;

    Session(String ledgerName, QldbSessionV2AsyncClient client) {
        this.ledgerName = ledgerName;
        this.client = client;
        this.connectionFuture = new CompletableFuture<>();
        this.commandStreamSubject = PublishSubject.create();
        this.futures = new LinkedBlockingQueue<>();
        this.eventStreamSubscriber = new EventStreamSubscriber(futures);
    }

    CompletableFuture<SendCommandResponse> startSessionStream() {
        final SendCommandRequest sendCommandRequest = SendCommandRequest.builder().ledgerName(ledgerName).build();
        logger.debug("Sending SendCommand request: {}", sendCommandRequest);

        // TODO: Revisit back pressure strategy.
        client.sendCommand(sendCommandRequest, commandStreamSubject.toFlowable(BackpressureStrategy.ERROR), getResponseHandler());
        return connectionFuture;
     }

    private SendCommandResponseHandler getResponseHandler(){
        return SendCommandResponseHandler.builder()
            .onResponse(sendCommandResponse -> {
                logger.debug("Response handler received inital response {}", sendCommandResponse);
                sessionId = sendCommandResponse.sessionId();
                connectionFuture.complete(sendCommandResponse);
            })
            .onError(e -> {
                logger.error("An error occurred while establishing the connection or streaming the response: {}", e.getMessage(), e);
                if (!connectionFuture.isDone()){
                    connectionFuture.completeExceptionally(e);
                }
                else {
                    try {
                        final CompletableFuture<ResultStream> future = futures.poll(1000L, TimeUnit.MILLISECONDS);

                        if (future == null) throw QldbDriverException.create(Errors.FUTURE_QUEUE_EMTPY.get());

                        future.completeExceptionally(e);
                    } catch (Exception ex) {
                        logger.error("Errors completing future: {}", ex.getMessage(), ex);
                    }
                }
                eventStreamSubscriber.onError(e);
            })
            .onComplete(() -> {
                logger.debug("All data has been successfully published to event stream subscriber");
            })
            .onEventStream(publisher -> {
                logger.debug("Events are ready to be streamed to event subscriber");
                publisher.subscribe(eventStreamSubscriber);
            }).build();
    }

    private void send(CommandStream command) {
        logger.debug("Sending request: {}", command);
        commandStreamSubject.onNext(command);
    }

    CompletableFuture<ResultStream> sendStartTransaction() {
        final StartTransactionRequest startTransactionRequest = CommandStream.startTransactionBuilder().build();
        final CompletableFuture<ResultStream> future = new CompletableFuture<>();
        futures.offer(future);
        send(startTransactionRequest);
        return future;
    }

    CompletableFuture<ResultStream> sendExecute(String statement, List<IonValue> parameters, String txnId) {
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
        futures.offer(future);
        send(executeStatementRequest);
        return future;
    }

    CompletableFuture<ResultStream> sendFetchPage(String txnId, String nextPageToken) {
        final FetchPageRequest fetchPageRequest = CommandStream.fetchPageBuilder()
                .transactionId(txnId)
                .nextPageToken(nextPageToken)
                .build();
        final CompletableFuture<ResultStream> future = new CompletableFuture<>();
        futures.offer(future);
        send(fetchPageRequest);
        return future;
    }

    CompletableFuture<ResultStream> sendCommit(String txnId) {
        final CommitTransactionRequest commitTransactionRequest = CommandStream.commitTransactionBuilder()
                .transactionId(txnId)
                .build();
        final CompletableFuture<ResultStream> future = new CompletableFuture<>();
        futures.offer(future);
        send(commitTransactionRequest);
        return future;
    }

    CompletableFuture<ResultStream> sendAbort() {
        final AbortTransactionRequest abortTransactionRequest = CommandStream.abortTransactionBuilder().build();
        final CompletableFuture<ResultStream> future = new CompletableFuture<>();
        futures.offer(future);
        send(abortTransactionRequest);
        return future;
    }

    CompletableFuture<ResultStream>  sendEndSession() {
        final EndSessionRequest endSessionRequest = CommandStream.endSessionBuilder().build();
        final CompletableFuture<ResultStream> future = new CompletableFuture<>();
        futures.offer(future);
        send(endSessionRequest);
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
