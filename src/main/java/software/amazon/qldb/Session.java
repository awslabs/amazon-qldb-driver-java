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

import com.amazon.ion.IonValue;
import com.amazon.ion.IonWriter;
import com.amazon.ion.system.IonBinaryWriterBuilder;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.annotations.ThreadSafe;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.awssdk.services.qldbsession.QldbSessionClient;
import software.amazon.awssdk.services.qldbsession.model.AbortTransactionRequest;
import software.amazon.awssdk.services.qldbsession.model.AbortTransactionResult;
import software.amazon.awssdk.services.qldbsession.model.CommitTransactionRequest;
import software.amazon.awssdk.services.qldbsession.model.CommitTransactionResult;
import software.amazon.awssdk.services.qldbsession.model.EndSessionRequest;
import software.amazon.awssdk.services.qldbsession.model.EndSessionResult;
import software.amazon.awssdk.services.qldbsession.model.ExecuteStatementRequest;
import software.amazon.awssdk.services.qldbsession.model.ExecuteStatementResult;
import software.amazon.awssdk.services.qldbsession.model.FetchPageRequest;
import software.amazon.awssdk.services.qldbsession.model.FetchPageResult;
import software.amazon.awssdk.services.qldbsession.model.InvalidSessionException;
import software.amazon.awssdk.services.qldbsession.model.OccConflictException;
import software.amazon.awssdk.services.qldbsession.model.Page;
import software.amazon.awssdk.services.qldbsession.model.SendCommandRequest;
import software.amazon.awssdk.services.qldbsession.model.SendCommandResponse;
import software.amazon.awssdk.services.qldbsession.model.StartSessionRequest;
import software.amazon.awssdk.services.qldbsession.model.StartTransactionRequest;
import software.amazon.awssdk.services.qldbsession.model.StartTransactionResult;
import software.amazon.awssdk.services.qldbsession.model.ValueHolder;
import software.amazon.qldb.exceptions.Errors;
import software.amazon.qldb.exceptions.QldbDriverException;

/**
 * Session object representing a communication channel with QLDB.
 *
 * This object is thread-safe.
 */
@ThreadSafe
class Session implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(Session.class);

    private final String ledgerName;
    private final String sessionToken;
    private final String sessionId;
    private final QldbSessionClient client;
    private final Duration startTransactionTimeout;

    /**
     * Constructor for a session to a specific ledger.
     #
     * @param ledgerName
     *              The name of the ledger to create a session to.
     * @param sessionToken
     *              The unique identifying token for this session to the QLDB.
     * @param sessionId
     *              The initial request ID for this session to QLDB.
     * @param client
     *              The low-level session used for communication with QLDB.
     * @param startTransactionTimeout
     *              The duration to wait before timing out on starting a new transaction.
     */
    private Session(
            String ledgerName,
            String sessionToken,
            String sessionId,
            QldbSessionClient client,
            Duration startTransactionTimeout
    ) {
        this.ledgerName = ledgerName;
        this.client = client;
        this.sessionToken = sessionToken;
        this.sessionId = sessionId;
        this.startTransactionTimeout = startTransactionTimeout;
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
    static Session startSession(
            String ledgerName,
            QldbSessionClient client,
            Duration startSessionTimeout,
            Duration startTransactionTimeout
    ) {
        final StartSessionRequest request = StartSessionRequest.builder().ledgerName(ledgerName).build();
        final SendCommandRequest command = SendCommandRequest.builder().startSession(request)
            .overrideConfiguration(c -> c.apiCallTimeout(startSessionTimeout))
            .build();

        logger.debug("Sending start session request: {}", command);
        final SendCommandResponse result = client.sendCommand(command);
        final String sessionToken = result.startSession().sessionToken();
        final String sessionId = result.responseMetadata().requestId();

        return new Session(ledgerName, sessionToken, sessionId, client, startTransactionTimeout);
    }

    @Override
    public void close() {
        try {
            sendEndSession();
        } catch (SdkServiceException e) {
            // We will only log issues closing the session, as QLDB will clean them up after a timeout.
            logger.warn("Errors closing session: {}", e.getMessage(), e);
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

    /**
     * Get the session token.
     *
     * @return This session's token.
     */
    String getToken() {
        return sessionToken;
    }

    /**
     * Send an abort request to QLDB, rolling back any active changes and closing any open results.
     *
     * @return The result of the abort transaction request.
     */
    AbortTransactionResult sendAbort() {
        final AbortTransactionRequest request = AbortTransactionRequest.builder().build();

        SendCommandResponse result = send(SendCommandRequest.builder().abortTransaction(request));
        return result.abortTransaction();
    }

    /**
     * Send a commit request to QLDB, committing any active changes and closing any open results.
     *
     * @param txnId
     *              The unique ID of the transaction to commit.
     * @param transactionDigest
     *              The digest hash of the transaction to commit.
     *
     * @return The result of the commit transaction request.
     * @throws OccConflictException if an OCC conflict has been detected within the transaction.
     */
    CommitTransactionResult sendCommit(String txnId, ByteBuffer transactionDigest) {
        final CommitTransactionRequest request = CommitTransactionRequest.builder()
             .transactionId(txnId)
             .commitDigest(SdkBytes.fromByteBuffer(transactionDigest))
             .build();

        SendCommandResponse result = send(SendCommandRequest.builder().commitTransaction(request));
        return result.commitTransaction();
    }

    /**
     * Send an end session request to QLDB, closing all open results and transactions.
     *
     * @return The result of the end session request.
     */
    EndSessionResult sendEndSession() {
        final EndSessionRequest request = EndSessionRequest.builder().build();

        SendCommandResponse result = send(SendCommandRequest.builder().endSession(request));
        return result.endSession();
    }

    /**
     * Send an execute request with parameters to QLDB.
     *
     * @param statement
     *              The PartiQL statement to execute.
     * @param parameters
     *              The parameters to use with the PartiQL statement for execution.
     * @param txnId
     *              The unique ID of the transaction to execute.
     *
     * @return The result of the execution, which contains a {@link Page} representing the first data chunk.
     */
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

        final ExecuteStatementRequest request = ExecuteStatementRequest.builder()
                                                                       .statement(statement)
                                                                       .parameters(byteParameters)
                                                                       .transactionId(txnId)
                                                                       .build();
        SendCommandResponse result = send(SendCommandRequest.builder().executeStatement(request));
        return result.executeStatement();
    }

    /**
     * Send a fetch result request to QLDB, retrieving the next chunk of data for the result.

     * @param txnId
     *              The unique ID of the transaction to execute.
     * @param nextPageToken
     *              The token that indicates what the next expected page is.
     *
     * @return The result of the fetch page request.
     */
    FetchPageResult sendFetchPage(String txnId, String nextPageToken) {
        final FetchPageRequest request = FetchPageRequest.builder()
                                                         .transactionId(txnId)
                                                         .nextPageToken(nextPageToken)
                                                         .build();

        SendCommandResponse result = send(SendCommandRequest.builder().fetchPage(request));
        return result.fetchPage();
    }

    /**
     * Send a start transaction request to QLDB.
     *
     * @return The result of the start transaction request.
     */
    StartTransactionResult sendStartTransaction() {
        final StartTransactionRequest request = StartTransactionRequest.builder().build();
        final SendCommandRequest.Builder command = SendCommandRequest.builder().startTransaction(request);

        Instant now = Instant.now();
        logger.info("Starting transaction at: " + now.toString());
        try {
            final SendCommandResponse result = send(command, startTransactionTimeout);
            return result.startTransaction();
        } finally {
            Instant later = Instant.now();
            logger.info("Completed transaction at: " + later.toString());
            logger.info("Elapsed (transaction):  " + Duration.between(now, later).toSeconds());
        }
    }

    /**
     * Send a request to QLDB.
     *
     * @param request
     *              The request to send.
     *
     * @return The result returned by QLDB for the request.
     * @throws OccConflictException if an OCC conflict was detected when committing a transaction.
     * @throws InvalidSessionException when this session is invalid.
     */
    private SendCommandResponse send(SendCommandRequest.Builder request) {
        final SendCommandRequest command = request.sessionToken(sessionToken).build();
        logger.debug("Sending request: {}", command);
        return client.sendCommand(command);
    }

    /**
     * Send a request to QLDB, aborting early with the given timeout.
     *
     * @param request
     *              The request to send.
     * @param timeout
     *              The duration to wait before retrying the command (if a retry policy is set).
     *
     * @return The result returned by QLDB for the request.
     * @throws OccConflictException if an OCC conflict was detected when committing a transaction.
     * @throws InvalidSessionException when this session is invalid.
     */
    private SendCommandResponse send(SendCommandRequest.Builder request, Duration timeout) {
        final SendCommandRequest command = request.sessionToken(sessionToken).overrideConfiguration(c -> {
            c.apiCallTimeout(timeout);
        }).build();
        logger.debug("Sending request: {}, timeout limit: {}", command, timeout);
        return client.sendCommand(command);
    }
}
