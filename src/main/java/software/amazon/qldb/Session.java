package software.amazon.qldb;

import com.amazon.ion.IonValue;
import com.amazon.ion.IonWriter;
import com.amazon.ion.system.IonBinaryWriterBuilder;
import io.reactivex.rxjava3.core.BackpressureStrategy;
import io.reactivex.rxjava3.subjects.PublishSubject;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.annotations.NotThreadSafe;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.awssdk.services.qldbsessionv2.QldbSessionV2AsyncClient;
import software.amazon.awssdk.services.qldbsessionv2.model.AbortTransactionRequest;
import software.amazon.awssdk.services.qldbsessionv2.model.AbortTransactionResult;
import software.amazon.awssdk.services.qldbsessionv2.model.CommandStream;
import software.amazon.awssdk.services.qldbsessionv2.model.CommitTransactionRequest;
import software.amazon.awssdk.services.qldbsessionv2.model.CommitTransactionResult;
import software.amazon.awssdk.services.qldbsessionv2.model.EndSessionRequest;
import software.amazon.awssdk.services.qldbsessionv2.model.EndSessionResult;
import software.amazon.awssdk.services.qldbsessionv2.model.ExecuteStatementRequest;
import software.amazon.awssdk.services.qldbsessionv2.model.ExecuteStatementResult;
import software.amazon.awssdk.services.qldbsessionv2.model.FetchPageRequest;
import software.amazon.awssdk.services.qldbsessionv2.model.FetchPageResult;
import software.amazon.awssdk.services.qldbsessionv2.model.QldbSessionV2Exception;
import software.amazon.awssdk.services.qldbsessionv2.model.ResultStream;
import software.amazon.awssdk.services.qldbsessionv2.model.SendCommandRequest;
import software.amazon.awssdk.services.qldbsessionv2.model.SendCommandResponse;
import software.amazon.awssdk.services.qldbsessionv2.model.SendCommandResponseHandler;
import software.amazon.awssdk.services.qldbsessionv2.model.StartTransactionRequest;
import software.amazon.awssdk.services.qldbsessionv2.model.StartTransactionResult;
import software.amazon.awssdk.services.qldbsessionv2.model.StatementError;
import software.amazon.awssdk.services.qldbsessionv2.model.TransactionError;
import software.amazon.awssdk.services.qldbsessionv2.model.ValueHolder;
import software.amazon.qldb.exceptions.Errors;
import software.amazon.qldb.exceptions.ExecuteException;
import software.amazon.qldb.exceptions.QldbDriverException;
import software.amazon.qldb.exceptions.StatementException;
import software.amazon.qldb.exceptions.TransactionException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
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
    private static CompletableFuture<Void> future;
    private final String ledgerName;
    private final QldbSessionV2AsyncClient client;
    private static String sessionId;
    private static PublishSubject<CommandStream> commandStreamSubject;
    private static EventStreamSubscriber eventStreamSubscriber;
    private static LinkedBlockingQueue<SendCommandResponse> connection;

    private Session(String ledgerName, QldbSessionV2AsyncClient client) {
        this.ledgerName = ledgerName;
        this.client = client;
    }

    /**
     * Factory method for constructing a new Session, creating a new session to the QLDB on construction.
     *
     * @param ledgerName
     *              The name of the ledger to create a session to.
     * @param client
     *              The low-level session used for communication with QLDB.
     *
     * @return A newly created {@link Session}.
     */
    static Session startSession(String ledgerName, QldbSessionV2AsyncClient client) {
        final Session session = new Session(ledgerName, client);
        commandStreamSubject = PublishSubject.create();
        eventStreamSubscriber = new EventStreamSubscriber();
        connection = new LinkedBlockingQueue<>(1);
        future = startSessionStream(ledgerName, client);
        throwStreamException();
        return session;
    }

     static CompletableFuture<Void> startSessionStream(String ledgerName, QldbSessionV2AsyncClient client) {
        if (!connection.isEmpty()) {
            throw QldbDriverException.create(QldbDriverException.create(Errors.SESSION_STREAM_ALREADY_OPEN.get()));
        } else {
            final SendCommandRequest sendCommandRequest = SendCommandRequest.builder().ledgerName(ledgerName).build();
            logger.info("Start streaming...");
            return client.sendCommand(sendCommandRequest, commandStreamSubject.toFlowable(BackpressureStrategy.ERROR), getResponseHandler());
        }
     }

    private static SendCommandResponseHandler getResponseHandler(){
        return SendCommandResponseHandler.builder()
            .onResponse(sendCommandResponse -> {
                System.out.println(Thread.currentThread().getName() + " SendCommandResponseHandler: Received initial response " + sendCommandResponse);
                // It'd be great if there is a way to distinguish whether the connection is established out of future.
                // e.g. Server could return CompletableFuture<SendCommandResponse>...
                connection.offer(sendCommandResponse);
                sessionId = sendCommandResponse.sessionId();
            })
            .onError(e -> {
                System.err.println(Thread.currentThread().getName() + " SendCommandResponseHandler: Error occurred while stream - " + e.getMessage());
                connection.clear();
            })
            .onComplete(() -> {
                System.out.println(Thread.currentThread().getName() + " SendCommandResponseHandler: Received complete");
                connection.clear();
            })
            .onEventStream(publisher -> {
                System.out.println(Thread.currentThread().getName() + " SendCommandResponseHandler: On event stream...");
                publisher.subscribe(eventStreamSubscriber);
            }).build();
    }

    private void send(CommandStream command) {
        System.out.println(Thread.currentThread().getName() + " Sending " + command);
        commandStreamSubject.onNext(command);
    }

    private void waitForConnection() {
        try {
            SendCommandResponse response = connection.poll(1000L, TimeUnit.MILLISECONDS);
            if (response == null) {
                throw QldbDriverException.create(Errors.SESSION_STREAM_NOT_EXIST.get());
            }
            connection.put(response);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw QldbDriverException.create(Errors.GET_CONNECTION_INTERRUPTED.get());
        }
    }

    // Check if stream is already completed.
    // If stream is done exceptionally, the exception will be routed to QldbDriverImpl.java because the session/stream is terminated.
    private static void throwStreamException() {
        if (future.isDone()) {
            try {
                future.get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw QldbDriverException.create(Errors.GET_SESSION_INTERRUPTED.get());
            } catch (ExecutionException ee) {
                final Throwable cause = ee.getCause();
                if (cause instanceof SdkClientException) {
                    // Give up dead session and retry with a new session.
                    // TODO: Resume session by sending a new SendCommandRequest.
                    throw new ExecuteException((RuntimeException) cause, true, false, false, "None");
                }
                else if (cause instanceof QldbSessionV2Exception) {
                    // Give up dead session and retry with a new session when it's 500/503.
                    final QldbSessionV2Exception serviceException = (QldbSessionV2Exception) cause;
                    boolean retryable = (serviceException.statusCode() == HttpStatus.SC_INTERNAL_SERVER_ERROR)
                        || (serviceException.statusCode() == HttpStatus.SC_SERVICE_UNAVAILABLE);
                    throw new ExecuteException(serviceException, retryable, false, false, "None");
                }
                else {
                    // Give up dead session and don't retry.
                    throw new ExecuteException((RuntimeException) ee.getCause(), false, false, false, "None");
                }
            }
        }
    }

    // Throw TransactionException and StatementException if result is an EventError.
    // The exception will be handled directly in QldbSession.java.
    private ResultStream throwEventException(ResultStream result) {
        throwStreamException();

        if (result == null) {
            throw QldbDriverException.create(Errors.RESPONSE_QUEUE_EMTPY.get());
        }
        else if (result instanceof TransactionError) {
            throw TransactionException.create((TransactionError)result);
        } else if (result instanceof StatementError) {
            throw StatementException.create((StatementError)result);
        } else return result;
    }

    StartTransactionResult sendStartTransaction() {
        waitForConnection();

        try {
            final StartTransactionRequest startTransactionRequest = CommandStream.startTransactionBuilder().build();
            send(startTransactionRequest);
            ResultStream result = throwEventException(eventStreamSubscriber.waitForResult());
            System.out.println(Thread.currentThread().getName() + " Got command response: " + result);
            return (StartTransactionResult) result;
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw QldbDriverException.create(Errors.GET_COMMAND_RESULT_INTERRUPTED.get(), ie.getCause());
        }
    }

    ExecuteStatementResult sendExecute(String statement, List<IonValue> parameters, String txnId) {
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
        waitForConnection();
        try {
            final ExecuteStatementRequest executeStatementRequest = CommandStream.executeStatementBuilder()
                    .statement(statement)
                    .parameters(byteParameters)
                    .transactionId(txnId)
                    .build();
            send(executeStatementRequest);
            ResultStream result = throwEventException(eventStreamSubscriber.waitForResult());
            System.out.println(Thread.currentThread().getName() + " Got command response: " + result);
            return (ExecuteStatementResult) result;
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw QldbDriverException.create(Errors.GET_COMMAND_RESULT_INTERRUPTED.get());
        }
    }

    FetchPageResult sendFetchPage(String txnId, String nextPageToken) {
        waitForConnection();

        try {
            final FetchPageRequest fetchPageRequest = CommandStream.fetchPageBuilder()
                .transactionId(txnId)
                .nextPageToken(nextPageToken)
                .build();

            send(fetchPageRequest);
            ResultStream result = throwEventException(eventStreamSubscriber.waitForResult());
            System.out.println(Thread.currentThread().getName() + " Got command response: " + result);
            return (FetchPageResult) result;
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw QldbDriverException.create(Errors.GET_COMMAND_RESULT_INTERRUPTED.get());
        }
    }

    CommitTransactionResult sendCommit(String txnId) {
        waitForConnection();

        try {
            final CommitTransactionRequest commitTransactionRequest = CommandStream.commitTransactionBuilder()
                    .transactionId(txnId)
                    .build();
            send(commitTransactionRequest);
            ResultStream result = throwEventException(eventStreamSubscriber.waitForResult());
            System.out.println(Thread.currentThread().getName() + " Got command response: " + result);
            return (CommitTransactionResult) result;
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw QldbDriverException.create(Errors.GET_COMMAND_RESULT_INTERRUPTED.get());
        }
    }

    AbortTransactionResult sendAbort() {
        waitForConnection();

        try {
            final AbortTransactionRequest abortTransactionRequest = CommandStream.abortTransactionBuilder().build();

            send(abortTransactionRequest);
            ResultStream result = throwEventException(eventStreamSubscriber.waitForResult());
            System.out.println(Thread.currentThread().getName() + " Got command response: " + result);
            return (AbortTransactionResult) result;
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw QldbDriverException.create(Errors.GET_COMMAND_RESULT_INTERRUPTED.get());
        }
    }

    EndSessionResult sendEndSession() {
        waitForConnection();

        try {
            final EndSessionRequest endSessionRequest = CommandStream.endSessionBuilder().build();
            send(endSessionRequest);
            ResultStream result = throwEventException(eventStreamSubscriber.waitForResult());
            System.out.println(Thread.currentThread().getName() + " Got command response: " + result);
            return (EndSessionResult) result;
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw QldbDriverException.create(Errors.GET_COMMAND_RESULT_INTERRUPTED.get());
        }
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
            sendEndSession();
        } catch (SdkServiceException e) {
            // We will only log issues closing the session, as QLDB will clean them up after a timeout.
            logger.warn("Errors closing session: {}", e.getMessage(), e);
        }
    }

}
