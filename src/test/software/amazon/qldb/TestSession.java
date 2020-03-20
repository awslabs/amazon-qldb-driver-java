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
import com.amazonaws.ResponseMetadata;
import com.amazonaws.services.qldbsession.AmazonQLDBSession;
import com.amazonaws.services.qldbsession.model.AbortTransactionRequest;
import com.amazonaws.services.qldbsession.model.CommitTransactionRequest;
import com.amazonaws.services.qldbsession.model.CommitTransactionResult;
import com.amazonaws.services.qldbsession.model.EndSessionRequest;
import com.amazonaws.services.qldbsession.model.ExecuteStatementRequest;
import com.amazonaws.services.qldbsession.model.ExecuteStatementResult;
import com.amazonaws.services.qldbsession.model.FetchPageRequest;
import com.amazonaws.services.qldbsession.model.FetchPageResult;
import com.amazonaws.services.qldbsession.model.InvalidSessionException;
import com.amazonaws.services.qldbsession.model.SendCommandRequest;
import com.amazonaws.services.qldbsession.model.SendCommandResult;
import com.amazonaws.services.qldbsession.model.StartSessionRequest;
import com.amazonaws.services.qldbsession.model.StartSessionResult;
import com.amazonaws.services.qldbsession.model.StartTransactionRequest;
import com.amazonaws.services.qldbsession.model.StartTransactionResult;
import com.amazonaws.services.qldbsession.model.ValueHolder;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import software.amazon.qldb.exceptions.QldbClientException;

public class TestSession {
    private static final String MOCK_LEDGER_NAME = "ledger";
    private static final String MOCK_SESSION_TOKEN = "token";
    private static final String MOCK_REQUEST_ID = "requestId";
    private static final String MOCK_TXN_ID = "txnId";
    private static final String MOCK_STATEMENT = "SELECT * FROM foo";
    private static final String MOCK_NEXT_PAGE_TOKEN = "nextResultToken";
    private static final ByteBuffer MOCK_TXN_DIGEST = ByteBuffer.wrap("foo".getBytes());

    @Mock
    private AmazonQLDBSession mockClient;

    @Mock
    private SendCommandResult mockSendCommandResult;

    @Mock
    private CommitTransactionResult mockCommitTransactionResult;

    @Mock
    private StartSessionResult mockStartSessionResult;

    @Mock
    private StartTransactionResult mockStartTransactionResult;

    @Mock
    private ExecuteStatementResult mockExecuteStatementResult;

    @Mock
    private FetchPageResult mockFetchPageResult;

    @Mock
    private ResponseMetadata mockSdkResponseMetadata;

    @Mock
    private final IonValue mockIonValue = Mockito.mock(IonValue.class);

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void init() {
        MockitoAnnotations.initMocks(this);

        Mockito.when(mockStartSessionResult.getSessionToken()).thenReturn(MOCK_SESSION_TOKEN);
        Mockito.when(mockSendCommandResult.getStartSession()).thenReturn(mockStartSessionResult);
        Mockito.when(mockSendCommandResult.getCommitTransaction()).thenReturn(mockCommitTransactionResult);
        Mockito.when(mockClient.sendCommand(ArgumentMatchers.any(SendCommandRequest.class)))
                .thenReturn(mockSendCommandResult);
        Mockito.when(mockSendCommandResult.getSdkResponseMetadata()).thenReturn(mockSdkResponseMetadata);
        Mockito.when(mockSendCommandResult.getSdkResponseMetadata().getRequestId()).thenReturn(MOCK_REQUEST_ID);
    }

    @Test
    public void testStartSession() {
        final ArgumentCaptor<SendCommandRequest> commandCaptor = ArgumentCaptor.forClass(SendCommandRequest.class);
        final StartSessionRequest startSessionRequest = new StartSessionRequest().withLedgerName(MOCK_LEDGER_NAME);
        final SendCommandRequest startSessionCommand = new SendCommandRequest().withStartSession(startSessionRequest);

        Session.startSession(MOCK_LEDGER_NAME, mockClient);

        Mockito.verify(mockClient, Mockito.times(1)).sendCommand(commandCaptor.capture());
        Assert.assertEquals(startSessionCommand, commandCaptor.getValue());
    }

    @Test
    public void testSendAbort() {
        final ArgumentCaptor<SendCommandRequest> commandCaptor = ArgumentCaptor.forClass(SendCommandRequest.class);
        final AbortTransactionRequest sendAbortRequest = new AbortTransactionRequest();
        final SendCommandRequest sendAbortCommand = new SendCommandRequest().withAbortTransaction(sendAbortRequest)
                .withSessionToken(MOCK_SESSION_TOKEN);

        final Session session = Session.startSession(MOCK_LEDGER_NAME, mockClient);
        session.sendAbort();

        Mockito.verify(mockClient, Mockito.times(2)).sendCommand(commandCaptor.capture());
        Mockito.verify(mockSendCommandResult).getAbortTransaction();
        Assert.assertEquals(sendAbortCommand, commandCaptor.getValue());
    }

    @Test
    public void testSendCommit() {
        final ArgumentCaptor<SendCommandRequest> commandCaptor = ArgumentCaptor.forClass(SendCommandRequest.class);
        final CommitTransactionRequest sendCommitRequest = new CommitTransactionRequest().withTransactionId(MOCK_TXN_ID)
                .withCommitDigest(MOCK_TXN_DIGEST);
        final SendCommandRequest sendCommitCommand = new SendCommandRequest().withCommitTransaction(sendCommitRequest)
                .withSessionToken(MOCK_SESSION_TOKEN);

        final Session session = Session.startSession(MOCK_LEDGER_NAME, mockClient);
        session.sendCommit(MOCK_TXN_ID, MOCK_TXN_DIGEST);

        Mockito.verify(mockClient, Mockito.times(2)).sendCommand(commandCaptor.capture());
        Mockito.verify(mockSendCommandResult).getCommitTransaction();
        Assert.assertEquals(sendCommitCommand, commandCaptor.getValue());
    }

    @Test
    public void testSendEndSession() {
        final ArgumentCaptor<SendCommandRequest> commandCaptor = ArgumentCaptor.forClass(SendCommandRequest.class);

        final EndSessionRequest endSessionRequest = new EndSessionRequest();
        final SendCommandRequest endSessionCommand = new SendCommandRequest().withEndSession(endSessionRequest)
                .withSessionToken(MOCK_SESSION_TOKEN);

        final Session session = Session.startSession(MOCK_LEDGER_NAME, mockClient);
        session.sendEndSession();

        Mockito.verify(mockClient, Mockito.times(2)).sendCommand(commandCaptor.capture());
        Mockito.verify(mockSendCommandResult).getEndSession();
        Assert.assertEquals(endSessionCommand, commandCaptor.getValue());
    }

    @Test
    public void testSendExecuteWithNoParameters() {
        Mockito.when(mockSendCommandResult.getExecuteStatement()).thenReturn(mockExecuteStatementResult);

        final ArgumentCaptor<SendCommandRequest> commandCaptor = ArgumentCaptor.forClass(SendCommandRequest.class);
        final List<ValueHolder> byteParameters = new ArrayList<>(0);
        final ExecuteStatementRequest executeRequest = new ExecuteStatementRequest()
                .withStatement(MOCK_STATEMENT)
                .withParameters(byteParameters)
                .withTransactionId(MOCK_TXN_ID);
        final SendCommandRequest executeCommand = new SendCommandRequest().withExecuteStatement(executeRequest)
                .withSessionToken(MOCK_SESSION_TOKEN);

        final Session session = Session.startSession(MOCK_LEDGER_NAME, mockClient);
        session.sendExecute(MOCK_STATEMENT, Collections.emptyList(), MOCK_TXN_ID);

        Mockito.verify(mockClient, Mockito.times(2)).sendCommand(commandCaptor.capture());
        Mockito.verify(mockSendCommandResult).getExecuteStatement();
        Assert.assertEquals(executeCommand, commandCaptor.getValue());
    }

    @Test
    public void testSendExecuteWithParameters() throws IOException {
        Mockito.when(mockSendCommandResult.getExecuteStatement()).thenReturn(mockExecuteStatementResult);

        final List<IonValue> MOCK_PARAMETERS = Collections.singletonList(mockIonValue);
        final ArgumentCaptor<SendCommandRequest> commandCaptor = ArgumentCaptor.forClass(SendCommandRequest.class);
        final ExecuteStatementRequest executeRequest = new ExecuteStatementRequest()
                .withStatement(MOCK_STATEMENT)
                .withParameters(MockResponses.createByteValues(MOCK_PARAMETERS))
                .withTransactionId(MOCK_TXN_ID);
        final SendCommandRequest executeCommand = new SendCommandRequest().withExecuteStatement(executeRequest)
                .withSessionToken(MOCK_SESSION_TOKEN);

        final Session session = Session.startSession(MOCK_LEDGER_NAME, mockClient);
        session.sendExecute(MOCK_STATEMENT, MOCK_PARAMETERS, MOCK_TXN_ID);

        Mockito.verify(mockClient, Mockito.times(2)).sendCommand(commandCaptor.capture());
        Mockito.verify(mockSendCommandResult).getExecuteStatement();
        Assert.assertEquals(executeCommand, commandCaptor.getValue());
    }

    @Test
    public void testSendExecuteRaisesException() {
        Mockito.when(mockSendCommandResult.getExecuteStatement()).thenReturn(mockExecuteStatementResult);
        Mockito.doAnswer(new Answer<String>() {
            @Override
            public String answer(InvocationOnMock invocation) throws Throwable {
                throw new IOException("msg");
            }
        }).when(mockIonValue).writeTo(ArgumentMatchers.any(IonWriter.class));

        final List<IonValue> MOCK_INVALID_PARAMETERS = Collections.singletonList(mockIonValue);

        final Session session = Session.startSession(MOCK_LEDGER_NAME, mockClient);

        thrown.expect(QldbClientException.class);

        try {
            session.sendExecute(MOCK_STATEMENT, MOCK_INVALID_PARAMETERS, MOCK_TXN_ID);
        } finally {
            Mockito.verify(mockClient, Mockito.times(1))
                    .sendCommand(ArgumentMatchers.any(SendCommandRequest.class));
        }
    }

    @Test
    public void testSendFetch() {
        Mockito.when(mockSendCommandResult.getFetchPage()).thenReturn(mockFetchPageResult);

        final ArgumentCaptor<SendCommandRequest> commandCaptor = ArgumentCaptor.forClass(SendCommandRequest.class);
        final FetchPageRequest fetchRequest = new FetchPageRequest()
                .withTransactionId(MOCK_TXN_ID).withNextPageToken(MOCK_NEXT_PAGE_TOKEN);
        final SendCommandRequest fetchCommand = new SendCommandRequest().withFetchPage(fetchRequest)
                .withSessionToken(MOCK_SESSION_TOKEN);

        final Session session = Session.startSession(MOCK_LEDGER_NAME, mockClient);
        session.sendFetchPage(MOCK_TXN_ID, MOCK_NEXT_PAGE_TOKEN);

        Mockito.verify(mockClient, Mockito.times(2)).sendCommand(commandCaptor.capture());
        Mockito.verify(mockSendCommandResult).getFetchPage();
        Assert.assertEquals(fetchCommand, commandCaptor.getValue());
    }

    @Test
    public void testSendStartTransaction() {
        Mockito.when(mockSendCommandResult.getStartTransaction()).thenReturn(mockStartTransactionResult);

        final StartTransactionRequest startTransactionRequest = new StartTransactionRequest();
        final SendCommandRequest startTransactionCommand = new SendCommandRequest()
                .withStartTransaction(startTransactionRequest)
                .withSessionToken(MOCK_SESSION_TOKEN);
        final ArgumentCaptor<SendCommandRequest> commandCaptor = ArgumentCaptor.forClass(SendCommandRequest.class);

        final Session session = Session.startSession(MOCK_LEDGER_NAME, mockClient);
        session.sendStartTransaction();

        Mockito.verify(mockClient, Mockito.times(2)).sendCommand(commandCaptor.capture());
        Mockito.verify(mockSendCommandResult).getStartTransaction();
        Assert.assertEquals(startTransactionCommand, commandCaptor.getValue());
    }

    @Test
    public void testGetClient() {
        final Session session = Session.startSession(MOCK_LEDGER_NAME, mockClient);

        Assert.assertEquals(session.getClient(), mockClient);
    }

    @Test
    public void testGetId() {
        final Session session = Session.startSession(MOCK_LEDGER_NAME, mockClient);

        Assert.assertEquals(session.getId(), MOCK_REQUEST_ID);
    }

    @Test
    public void testGetLedgerName() {
        final Session session = Session.startSession(MOCK_LEDGER_NAME, mockClient);

        Assert.assertEquals(session.getLedgerName(), MOCK_LEDGER_NAME);
    }

    @Test
    public void testGetToken() {
        final Session session = Session.startSession(MOCK_LEDGER_NAME, mockClient);
        
        Assert.assertEquals(session.getToken(), MOCK_SESSION_TOKEN);
    }

    @Test
    public void testInvalidSessionException() {
        final InvalidSessionException exception = new InvalidSessionException("msg");
        final Session session = Session.startSession(MOCK_LEDGER_NAME, mockClient);
        Mockito.when(mockClient.sendCommand(ArgumentMatchers.any(SendCommandRequest.class)))
                .thenThrow(exception);

        try {
            session.sendStartTransaction();
        } catch (InvalidSessionException ise) {
            Assert.assertEquals(ise, exception);
        }

        try {
            session.sendStartTransaction();
        } catch (InvalidSessionException ise) {
            Assert.assertEquals(ise, exception);
        }
        Mockito.verify(mockClient, Mockito.times(3))
                .sendCommand(ArgumentMatchers.any(SendCommandRequest.class));
    }
}
