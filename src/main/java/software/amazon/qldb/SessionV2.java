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
import software.amazon.awssdk.core.async.SdkPublisher;
import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.awssdk.services.qldbsessionv2.QldbSessionV2AsyncClient;
import software.amazon.awssdk.services.qldbsessionv2.model.AbortTransactionRequest;
import software.amazon.awssdk.services.qldbsessionv2.model.AbortTransactionResult;
import software.amazon.awssdk.services.qldbsessionv2.model.CommandResult;
import software.amazon.awssdk.services.qldbsessionv2.model.CommandStream;
import software.amazon.awssdk.services.qldbsessionv2.model.CommitTransactionRequest;
import software.amazon.awssdk.services.qldbsessionv2.model.CommitTransactionResult;
import software.amazon.awssdk.services.qldbsessionv2.model.EndSessionRequest;
import software.amazon.awssdk.services.qldbsessionv2.model.EndSessionResult;
import software.amazon.awssdk.services.qldbsessionv2.model.ExecuteStatementRequest;
import software.amazon.awssdk.services.qldbsessionv2.model.ExecuteStatementResult;
import software.amazon.awssdk.services.qldbsessionv2.model.FetchPageResult;
import software.amazon.awssdk.services.qldbsessionv2.model.ResultStream;
import software.amazon.awssdk.services.qldbsessionv2.model.SendCommandRequest;
import software.amazon.awssdk.services.qldbsessionv2.model.SendCommandResponse;
import software.amazon.awssdk.services.qldbsessionv2.model.SendCommandResponseHandler;
import software.amazon.awssdk.services.qldbsessionv2.model.StartTransactionRequest;
import software.amazon.awssdk.services.qldbsessionv2.model.StartTransactionResult;
import software.amazon.awssdk.services.qldbsessionv2.model.ValueHolder;
import software.amazon.qldb.exceptions.Errors;
import software.amazon.qldb.exceptions.QldbDriverException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;


/**
 * Session object representing a communication channel with QLDB.
 *
 * This object is thread-safe.
 */
@NotThreadSafe
class SessionV2 {
    private static final Logger logger = LoggerFactory.getLogger(SessionV2.class);
    private final String ledgerName;
    private final QldbSessionV2AsyncClient client;
    private final PublishSubject<CommandStream> commandStreamSubject;
    private final ResultStreamSubscriber resultStreamSubscriber;
    private final LinkedBlockingQueue<SendCommandResponse> connection;

    public SessionV2(String ledgerName, QldbSessionV2AsyncClient client) {
        this.ledgerName = ledgerName;
        this.client = client;
        this.commandStreamSubject = PublishSubject.create();
        this.resultStreamSubscriber = new ResultStreamSubscriber();
        this.connection = new LinkedBlockingQueue<>(1);
    }

    private CompletableFuture<Void> connect() {
        final SendCommandRequest sendCommandRequest = SendCommandRequest.builder().ledgerName(ledgerName).build();

        return client.sendCommand(sendCommandRequest, commandStreamSubject.toFlowable(BackpressureStrategy.ERROR), new SendCommandResponseHandler() {

            @Override
            public void responseReceived(SendCommandResponse response) {
                System.out.println(Thread.currentThread().getName() + " SendCommandResponseHandler: Received SendCommand response " + response);
                try {
                    if(!connection.offer(response, 5000L, TimeUnit.MILLISECONDS)){
                        System.out.println("Gave up the new connection since there is an existing connection");
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            @Override
            public void onEventStream(SdkPublisher<ResultStream> publisher) {
                System.out.println(Thread.currentThread().getName() + " SendCommandResponseHandler: On event stream...");
                publisher.subscribe(resultStreamSubscriber);

            }

            @Override
            public void exceptionOccurred(Throwable throwable) {
                System.err.println("SendCommandResponseHandler: Error occurred while stream - " + throwable.getMessage());
            }

            @Override
            public void complete() {
                System.out.println("SendCommandResponseHandler: Received complete");
            }
        });
    }

    private void send(CommandStream command) {
        System.out.println(Thread.currentThread().getName() + " Sending " + command);
        commandStreamSubject.onNext(command);
    }

    CompletableFuture<Void> startConnection() {
        System.out.println(Thread.currentThread().getName() + "Start connection...");

        CompletableFuture<Void> future = connect();

        try {
            if(connection.poll(5000L, TimeUnit.MILLISECONDS) == null){
                System.out.println("Timeout establishing connection.");
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return future;
    }

    StartTransactionResult sendStartTransaction() {

        try {
            final StartTransactionRequest startTransactionRequest = CommandStream.startTransactionBuilder().build();
            send(startTransactionRequest);
            CommandResult commandResult = resultStreamSubscriber.waitForResult();
            System.out.println(Thread.currentThread().getName() + " Got command response: " + commandResult);
            return commandResult.startTransaction();
        } catch (InterruptedException e) {
            e.printStackTrace();
            return null;
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

        try {
            final ExecuteStatementRequest executeStatementRequest = CommandStream.executeStatementBuilder()
                    .statement(statement)
                    .parameters(byteParameters)
                    .transactionId(txnId)
                    .build();
            send(executeStatementRequest);
            CommandResult commandResult = resultStreamSubscriber.waitForResult();
            System.out.println(Thread.currentThread().getName() + ": Got command response: " + commandResult);
            return commandResult.executeStatement();
        } catch (InterruptedException e) {
            e.printStackTrace();
            return null;
        }
    }

//    FetchPageResult sendFetchPage(String txnId, String nextPageToken) {
//        final FetchPageRequest fetchPageRequest = FetchPageRequest.builder()
//                .transactionId(txnId)
//                .nextPageToken(nextPageToken)
//                .build();
//
//        send(fetchPageRequest);
//        CommandResult commandResult = null;
//        try {
//            commandResult = resultStreamSubscriber.waitForResult();
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//        assert commandResult != null;
//        return commandResult.fetchPage();
//    }

    LinkedBlockingQueue<FetchPageResult> fetchPages(String nextPageToken) {
        LinkedBlockingQueue<FetchPageResult> results = resultStreamSubscriber.getPages(nextPageToken);

        System.out.println(Thread.currentThread().getName() + ": Got pages from buffer: " + results);
        return results;
    }

    CommitTransactionResult sendCommit(String txnId, ByteBuffer transactionDigest) {
        try {
            final CommitTransactionRequest commitTransactionRequest = CommandStream.commitTransactionBuilder()
                    .transactionId(txnId)
                    .commitDigest(SdkBytes.fromByteBuffer(transactionDigest))
                    .build();
            send(commitTransactionRequest);
            CommandResult commandResult = resultStreamSubscriber.waitForResult();
            System.out.println(Thread.currentThread().getName() + " Got command response: " + commandResult);
            return commandResult.commitTransaction();
        } catch (InterruptedException e) {
            e.printStackTrace();
            return null;
        }
    }

    AbortTransactionResult sendAbort() {
        final AbortTransactionRequest abortTransactionRequest = CommandStream.abortTransactionBuilder().build();

        send(abortTransactionRequest);
        CommandResult commandResult = null;
        try {
            commandResult = resultStreamSubscriber.waitForResult();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        assert commandResult != null;
        return commandResult.abortTransaction();
    }

    EndSessionResult sendEndSession() {
        final EndSessionRequest endSessionRequest = EndSessionRequest.builder().build();

        send(endSessionRequest);
            try {
                CommandResult commandResult = resultStreamSubscriber.waitForResult();
                System.out.println(Thread.currentThread().getName() + " Got command response: " + commandResult);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        return null;
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
