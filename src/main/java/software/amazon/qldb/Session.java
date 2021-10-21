package software.amazon.qldb;

import com.amazon.ion.IonValue;
import com.amazon.ion.IonWriter;
import com.amazon.ion.system.IonBinaryWriterBuilder;
import io.reactivex.rxjava3.core.BackpressureStrategy;
import io.reactivex.rxjava3.subjects.PublishSubject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.annotations.ThreadSafe;
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
import software.amazon.awssdk.utils.CompletableFutureUtils;
import software.amazon.qldb.exceptions.Errors;
import software.amazon.qldb.exceptions.QldbDriverException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;


/**
 * Session object representing a communication channel with QLDB.
 *
 * This object is not thread-safe.
 */
@ThreadSafe
class Session {
    private static final Logger logger = LoggerFactory.getLogger(Session.class);
    private final String ledgerName;
    private final QldbSessionV2AsyncClient client;
    private final CompletableFuture<SendCommandResponse> connectionFuture;
    private final PublishSubject<CommandStream> commandStreamSubject;
    private final ResultStreamSubscriber resultStreamSubscriber;
    private CompletableFuture<Void> streamFuture;
    private String sessionId;

    Session(String ledgerName, QldbSessionV2AsyncClient client) {
        this.ledgerName = ledgerName;
        this.client = client;
        this.connectionFuture = new CompletableFuture<>();
        this.commandStreamSubject = PublishSubject.create();
        this.resultStreamSubscriber = new ResultStreamSubscriber();
    }

    CompletableFuture<SendCommandResponse> startSessionStream() {
        final SendCommandRequest sendCommandRequest = SendCommandRequest.builder().ledgerName(ledgerName).build();
        logger.debug("Sending SendCommand request: {}", sendCommandRequest);
        streamFuture = client.sendCommand(sendCommandRequest, commandStreamSubject.toFlowable(BackpressureStrategy.ERROR), getResponseHandler());
        return connectionFuture;
     }

    private SendCommandResponseHandler getResponseHandler(){
        return SendCommandResponseHandler.builder()
            .onResponse(sendCommandResponse -> {
                System.out.println(Thread.currentThread().getName() + " SendCommandResponseHandler: Received initial response " + sendCommandResponse);
                sessionId = sendCommandResponse.sessionId();
                connectionFuture.complete(sendCommandResponse);
            })
            .onError(e -> {
                System.err.println(Thread.currentThread().getName() + " SendCommandResponseHandler: Error occurred while stream - " + e.getMessage());
                if (!connectionFuture.isDone()) connectionFuture.completeExceptionally(e);
                else streamFuture.completeExceptionally(e);
            })
            .onComplete(() -> {
                System.out.println(Thread.currentThread().getName() + " SendCommandResponseHandler: Received complete");
                streamFuture.complete(null);
            })
            .onEventStream(publisher -> {
                System.out.println(Thread.currentThread().getName() + " SendCommandResponseHandler: On event stream...");
                publisher.subscribe(resultStreamSubscriber);
            }).build();
    }

    private void send(CommandStream command) {
        logger.debug("Sending request: {}", command);
        commandStreamSubject.onNext(command);
    }

    CompletableFuture<ResultStream> sendStartTransaction() throws InterruptedException {
        final StartTransactionRequest startTransactionRequest = CommandStream.startTransactionBuilder().build();
        send(startTransactionRequest);
        CompletableFuture<ResultStream> resultFuture = resultStreamSubscriber.pollResultFutureFromQueue();
        if (streamFuture.isCompletedExceptionally()) CompletableFutureUtils.forwardExceptionTo(streamFuture, resultFuture);
        return resultFuture;
    }

    CompletableFuture<ResultStream> sendExecute(String statement, List<IonValue> parameters, String txnId) throws InterruptedException {
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
        send(executeStatementRequest);
        CompletableFuture<ResultStream> resultFuture = resultStreamSubscriber.pollResultFutureFromQueue();
        if (streamFuture.isCompletedExceptionally()) CompletableFutureUtils.forwardExceptionTo(streamFuture, resultFuture);
        return resultFuture;
    }

    CompletableFuture<ResultStream> sendFetchPage(String txnId, String nextPageToken) throws InterruptedException {
        final FetchPageRequest fetchPageRequest = CommandStream.fetchPageBuilder()
                .transactionId(txnId)
                .nextPageToken(nextPageToken)
                .build();

        send(fetchPageRequest);
        CompletableFuture<ResultStream> resultFuture = resultStreamSubscriber.pollResultFutureFromQueue();
        if (streamFuture.isCompletedExceptionally()) CompletableFutureUtils.forwardExceptionTo(streamFuture, resultFuture);
        return resultFuture;
    }

    CompletableFuture<ResultStream> sendCommit(String txnId) throws InterruptedException {
        final CommitTransactionRequest commitTransactionRequest = CommandStream.commitTransactionBuilder()
                .transactionId(txnId)
                .build();

        send(commitTransactionRequest);
        CompletableFuture<ResultStream> resultFuture = resultStreamSubscriber.pollResultFutureFromQueue();
        if (streamFuture.isCompletedExceptionally()) CompletableFutureUtils.forwardExceptionTo(streamFuture, resultFuture);
        return resultFuture;
    }

    CompletableFuture<ResultStream> sendAbort() throws InterruptedException {
        final AbortTransactionRequest abortTransactionRequest = CommandStream.abortTransactionBuilder().build();

        send(abortTransactionRequest);
        CompletableFuture<ResultStream> resultFuture = resultStreamSubscriber.pollResultFutureFromQueue();
        if (streamFuture.isCompletedExceptionally()) CompletableFutureUtils.forwardExceptionTo(streamFuture, resultFuture);
        return resultFuture;
    }

    CompletableFuture<ResultStream>  sendEndSession() throws InterruptedException {
        final EndSessionRequest endSessionRequest = CommandStream.endSessionBuilder().build();
        send(endSessionRequest);
        CompletableFuture<ResultStream> resultFuture = resultStreamSubscriber.pollResultFutureFromQueue();
        if (streamFuture.isCompletedExceptionally()) CompletableFutureUtils.forwardExceptionTo(streamFuture, resultFuture);
        return resultFuture;
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
