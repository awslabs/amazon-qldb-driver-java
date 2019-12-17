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
import com.amazonaws.AmazonClientException;
import com.amazonaws.annotation.ThreadSafe;
import com.amazonaws.services.qldbsession.AmazonQLDBSession;
import com.amazonaws.services.qldbsession.model.AbortTransactionRequest;
import com.amazonaws.services.qldbsession.model.AbortTransactionResult;
import com.amazonaws.services.qldbsession.model.CommitTransactionRequest;
import com.amazonaws.services.qldbsession.model.CommitTransactionResult;
import com.amazonaws.services.qldbsession.model.EndSessionRequest;
import com.amazonaws.services.qldbsession.model.EndSessionResult;
import com.amazonaws.services.qldbsession.model.ExecuteStatementRequest;
import com.amazonaws.services.qldbsession.model.ExecuteStatementResult;
import com.amazonaws.services.qldbsession.model.FetchPageRequest;
import com.amazonaws.services.qldbsession.model.FetchPageResult;
import com.amazonaws.services.qldbsession.model.InvalidSessionException;
import com.amazonaws.services.qldbsession.model.OccConflictException;
import com.amazonaws.services.qldbsession.model.Page;
import com.amazonaws.services.qldbsession.model.SendCommandRequest;
import com.amazonaws.services.qldbsession.model.SendCommandResult;
import com.amazonaws.services.qldbsession.model.StartSessionRequest;
import com.amazonaws.services.qldbsession.model.StartTransactionRequest;
import com.amazonaws.services.qldbsession.model.StartTransactionResult;
import com.amazonaws.services.qldbsession.model.ValueHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.qldb.exceptions.Errors;
import software.amazon.qldb.exceptions.QldbClientException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

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
    private final AmazonQLDBSession client;

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
     */
    private Session(String ledgerName, String sessionToken, String sessionId, AmazonQLDBSession client) {
        this.ledgerName = ledgerName;
        this.client = client;
        this.sessionToken = sessionToken;
        this.sessionId = sessionId;
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
    static Session startSession(String ledgerName, AmazonQLDBSession client) {
        final StartSessionRequest request = new StartSessionRequest().withLedgerName(ledgerName);
        final SendCommandRequest command = new SendCommandRequest().withStartSession(request);

        logger.debug("Sending start session request: {}", command);
        final SendCommandResult result = client.sendCommand(command);
        final String sessionToken = result.getStartSession().getSessionToken();
        final String sessionId = result.getSdkResponseMetadata().getRequestId();

        return new Session(ledgerName, sessionToken, sessionId, client);
    }

    @Override
    public void close() {
        try {
            sendEndSession();
        } catch (AmazonClientException e) {
            // We will only log issues closing the session, as QLDB will clean them up after a timeout.
            logger.warn("Errors closing session: " + e.getMessage(), e);
        }
    }

    /**
     * Get the low-level session client used for communication with QLDB.
     *
     * @return The low-level session.
     */
    AmazonQLDBSession getClient() {
        return client;
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
     * Get the name of the ledger that this session is connected to.
     *
     * @return The name of the ledger this session is connected to.
     */
    String getLedgerName() {
        return ledgerName;
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
        final AbortTransactionRequest request = new AbortTransactionRequest();

        SendCommandResult result = send(new SendCommandRequest().withAbortTransaction(request));
        return result.getAbortTransaction();
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
        final CommitTransactionRequest request = new CommitTransactionRequest()
                .withTransactionId(txnId)
                .withCommitDigest(transactionDigest);

        SendCommandResult result = send(new SendCommandRequest().withCommitTransaction(request));
        return result.getCommitTransaction();
    }

    /**
     * Send an end session request to QLDB, closing all open results and transactions.
     *
     * @return The result of the end session request.
     */
    EndSessionResult sendEndSession() {
        final EndSessionRequest request = new EndSessionRequest();

        SendCommandResult result = send(new SendCommandRequest().withEndSession(request));
        return result.getEndSession();
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
                    byteParameters.add(new ValueHolder().withIonBinary(ByteBuffer.wrap(stream.toByteArray())));

                    // Reset the stream so that it can be re-used.
                    stream.reset();
                }
            } catch (IOException e) {
                throw QldbClientException.create(String.format(Errors.SERIALIZING_PARAMS.get(), e.getMessage()),
                        e, logger);
            }
        }

        final ExecuteStatementRequest request = new ExecuteStatementRequest()
                .withStatement(statement)
                .withParameters(byteParameters)
                .withTransactionId(txnId);
        SendCommandResult result = send(new SendCommandRequest().withExecuteStatement(request));
        return result.getExecuteStatement();
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
        final FetchPageRequest request = new FetchPageRequest()
                .withTransactionId(txnId)
                .withNextPageToken(nextPageToken);

        SendCommandResult result = send(new SendCommandRequest().withFetchPage(request));
        return result.getFetchPage();
    }

    /**
     * Send a start transaction request to QLDB.
     *
     * @return The result of the start transaction request.
     */
    StartTransactionResult sendStartTransaction() {
        final StartTransactionRequest request = new StartTransactionRequest();
        final SendCommandRequest command = new SendCommandRequest().withStartTransaction(request);

        final SendCommandResult result = send(command);
        return result.getStartTransaction();
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
    private SendCommandResult send(SendCommandRequest request) {
        final SendCommandRequest command = request.withSessionToken(sessionToken);
        logger.debug("Sending request: {}", command);
        return client.sendCommand(command);
    }
}
