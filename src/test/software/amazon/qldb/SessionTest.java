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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.amazon.ion.IonValue;
import com.amazon.ion.IonWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import io.reactivex.rxjava3.core.BackpressureStrategy;
import io.reactivex.rxjava3.subjects.PublishSubject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.reactivestreams.Publisher;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.qldbsessionv2.QldbSessionV2AsyncClient;
import software.amazon.awssdk.services.qldbsessionv2.model.AbortTransactionRequest;
import software.amazon.awssdk.services.qldbsessionv2.model.AbortTransactionResult;
import software.amazon.awssdk.services.qldbsessionv2.model.CommandResult;
import software.amazon.awssdk.services.qldbsessionv2.model.CommandStream;
import software.amazon.awssdk.services.qldbsessionv2.model.CommitTransactionResult;
import software.amazon.awssdk.services.qldbsessionv2.model.ExecuteStatementResult;
import software.amazon.awssdk.services.qldbsessionv2.model.FetchPageResult;
import software.amazon.awssdk.services.qldbsessionv2.model.SendCommandRequest;
import software.amazon.awssdk.services.qldbsessionv2.model.SendCommandResponse;
import software.amazon.awssdk.services.qldbsessionv2.model.SendCommandResponseHandler;
import software.amazon.awssdk.services.qldbsessionv2.model.StartTransactionResult;
import software.amazon.qldb.exceptions.QldbDriverException;

//public class SessionTest {
//    private static final String MOCK_LEDGER_NAME = "ledger";
//    private static final String MOCK_SESSION_TOKEN = "token";
//    private static final String MOCK_REQUEST_ID = "requestId";
//    private static final String MOCK_TXN_ID = "txnId";
//    private static final String MOCK_STATEMENT = "SELECT * FROM foo";
//    private static final String MOCK_NEXT_PAGE_TOKEN = "nextResultToken";
//    private static final SdkBytes MOCK_TXN_DIGEST = SdkBytes.fromByteBuffer(ByteBuffer.wrap("foo".getBytes()));
//
//    @Mock
//    private QldbSessionV2AsyncClient mockClient;
//
//    @Mock
//    private PublishSubject<CommandStream> mockCommandStreamPublishSubject;
//
//    @Mock
//    private SendCommandRequest mockSendCommandRequest;
//
//    @Mock
//    private SendCommandResponseHandler mockSendCommandResponseHandler;
//
//    @Mock
//    private CommandResult mockCommandResult;
//
//    @Mock
//    private CommitTransactionResult mockCommitTransactionResult;
//
//    @Mock
//    private StartTransactionResult mockStartTransactionResult;
//
//    @Mock
//    private ExecuteStatementResult mockExecuteStatementResult;
//
//    @Mock
//    private FetchPageResult mockFetchPageResult;
//
//    @Mock
//    private AbortTransactionResult mockAbortTransactionResult;
//
//    @Mock
//    private final IonValue mockIonValue = Mockito.mock(IonValue.class);
//
//
//    @BeforeEach
//    public void init() {
//        MockitoAnnotations.initMocks(this);
//        Mockito.when(mockClient.sendCommand(mockSendCommandRequest, mockCommandStreamPublishSubject.toFlowable(BackpressureStrategy.ERROR), mockSendCommandResponseHandler)).thenReturn(mockSendCommandFuture);
//        Mockito.when(mockCommandResult.abortTransaction()).thenReturn(mockAbortTransactionResult);
//        Mockito.when(mockCommandResult.commitTransaction()).thenReturn(mockCommitTransactionResult);
//    }
//
//    @Test
//    public void testSendAbort() {
//        final ArgumentCaptor<SendCommandRequest> sendCommandRequestArgumentCaptor = ArgumentCaptor.forClass(SendCommandRequest.class);
//        final ArgumentCaptor<Publisher<CommandStream>> commandPublisherArgumentCaptor = ArgumentCaptor.forClass(Publisher.class);
//        final ArgumentCaptor<SendCommandResponseHandler> responseHandlerArgumentCaptor = ArgumentCaptor.forClass(SendCommandResponseHandler.class);
//        final ArgumentCaptor<CommandStream> commandStreamArgumentCaptor = ArgumentCaptor.forClass(CommandStream.class);
//
//        final AbortTransactionRequest abortTransactionRequest = CommandStream.abortTransactionBuilder().build();
//        final SendCommandRequest sendCommandRequest = SendCommandRequest.builder().ledgerName(MOCK_LEDGER_NAME).build();
//        final SessionV2 session = new SessionV2(MOCK_LEDGER_NAME, mockClient);
//        session.sendAbort();
//
//        Mockito.verify(mockClient).sendCommand(sendCommandRequestArgumentCaptor.capture(), commandPublisherArgumentCaptor.capture(), responseHandlerArgumentCaptor.capture());
//        Mockito.verify(mockCommandStreamPublishSubject).onNext(commandStreamArgumentCaptor.capture());
//        Mockito.verify(mockCommandResult).abortTransaction();
//        assertEquals(sendCommandRequest, sendCommandRequestArgumentCaptor.getValue());
//        assertEquals(abortTransactionRequest, commandStreamArgumentCaptor.getValue());
//    }

//    @Test
//    public void testSendCommit() {
//        final ArgumentCaptor<SendCommandRequest> commandCaptor = ArgumentCaptor.forClass(SendCommandRequest.class);
//        final CommitTransactionRequest sendCommitRequest = CommitTransactionRequest.builder().transactionId(MOCK_TXN_ID)
//                                                                                   .commitDigest(MOCK_TXN_DIGEST).build();
//        final SendCommandRequest sendCommitCommand = SendCommandRequest.builder().commitTransaction(sendCommitRequest)
//                                                                       .sessionToken(MOCK_SESSION_TOKEN).build();
//
//        final Session session = Session.startSession(MOCK_LEDGER_NAME, mockClient);
//        session.sendCommit(MOCK_TXN_ID, MOCK_TXN_DIGEST.asByteBuffer());
//
//        Mockito.verify(mockClient, Mockito.times(2)).sendCommand(commandCaptor.capture());
//        Mockito.verify(mockSendCommandResult).commitTransaction();
//        assertEquals(sendCommitCommand, commandCaptor.getValue());
//    }
//
//    @Test
//    public void testSendEndSession() {
//        final ArgumentCaptor<SendCommandRequest> commandCaptor = ArgumentCaptor.forClass(SendCommandRequest.class);
//
//        final EndSessionRequest endSessionRequest = EndSessionRequest.builder().build();
//        final SendCommandRequest endSessionCommand = SendCommandRequest.builder().endSession(endSessionRequest)
//                                                                       .sessionToken(MOCK_SESSION_TOKEN).build();
//
//        final Session session = Session.startSession(MOCK_LEDGER_NAME, mockClient);
//        session.sendEndSession();
//
//        Mockito.verify(mockClient, Mockito.times(2)).sendCommand(commandCaptor.capture());
//        Mockito.verify(mockSendCommandResult).endSession();
//        assertEquals(endSessionCommand, commandCaptor.getValue());
//    }
//
//    @Test
//    public void testSendExecuteWithNoParameters() {
//        Mockito.when(mockSendCommandResult.executeStatement()).thenReturn(mockExecuteStatementResult);
//
//        final ArgumentCaptor<SendCommandRequest> commandCaptor = ArgumentCaptor.forClass(SendCommandRequest.class);
//        final List<ValueHolder> byteParameters = new ArrayList<>(0);
//        final ExecuteStatementRequest executeRequest = ExecuteStatementRequest.builder()
//                                                                              .statement(MOCK_STATEMENT)
//                                                                              .parameters(byteParameters)
//                                                                              .transactionId(MOCK_TXN_ID)
//                                                                              .build();
//        final SendCommandRequest executeCommand = SendCommandRequest.builder()
//                                                                    .executeStatement(executeRequest)
//                                                                    .sessionToken(MOCK_SESSION_TOKEN)
//                                                                    .build();
//
//        final Session session = Session.startSession(MOCK_LEDGER_NAME, mockClient);
//        session.sendExecute(MOCK_STATEMENT, Collections.emptyList(), MOCK_TXN_ID);
//
//        Mockito.verify(mockClient, Mockito.times(2)).sendCommand(commandCaptor.capture());
//        Mockito.verify(mockSendCommandResult).executeStatement();
//        assertEquals(executeCommand, commandCaptor.getValue());
//    }
//
//    @Test
//    public void testSendExecuteparameters() throws IOException {
//        Mockito.when(mockSendCommandResult.executeStatement()).thenReturn(mockExecuteStatementResult);
//
//        final List<IonValue> MOCK_PARAMETERS = Collections.singletonList(mockIonValue);
//        final ArgumentCaptor<SendCommandRequest> commandCaptor = ArgumentCaptor.forClass(SendCommandRequest.class);
//        final ExecuteStatementRequest executeRequest = ExecuteStatementRequest.builder()
//                                                                              .statement(MOCK_STATEMENT)
//                                                                              .parameters(MockResponses.createByteValues(MOCK_PARAMETERS))
//                                                                              .transactionId(MOCK_TXN_ID)
//                                                                              .build();
//        final SendCommandRequest executeCommand = SendCommandRequest.builder().executeStatement(executeRequest)
//                                                                    .sessionToken(MOCK_SESSION_TOKEN)
//                                                                    .build();
//
//        final Session session = Session.startSession(MOCK_LEDGER_NAME, mockClient);
//        session.sendExecute(MOCK_STATEMENT, MOCK_PARAMETERS, MOCK_TXN_ID);
//
//        Mockito.verify(mockClient, Mockito.times(2)).sendCommand(commandCaptor.capture());
//        Mockito.verify(mockSendCommandResult).executeStatement();
//        assertEquals(executeCommand, commandCaptor.getValue());
//    }
//
//    @Test
//    public void testSendExecuteRaisesException() {
//        Mockito.when(mockSendCommandResult.executeStatement()).thenReturn(mockExecuteStatementResult);
//        Mockito.doAnswer(new Answer<String>() {
//            @Override
//            public String answer(InvocationOnMock invocation) throws Throwable {
//                throw new IOException("msg");
//            }
//        }).when(mockIonValue).writeTo(ArgumentMatchers.any(IonWriter.class));
//
//        final List<IonValue> MOCK_INVALID_PARAMETERS = Collections.singletonList(mockIonValue);
//
//        final Session session = Session.startSession(MOCK_LEDGER_NAME, mockClient);
//
//        assertThrows(QldbDriverException.class, () -> {
//            try {
//                session.sendExecute(MOCK_STATEMENT, MOCK_INVALID_PARAMETERS, MOCK_TXN_ID);
//            } finally {
//                Mockito.verify(mockClient, Mockito.times(1))
//                       .sendCommand(ArgumentMatchers.any(SendCommandRequest.class));
//            }
//        });
//    }
//
//    @Test
//    public void testSendFetch() {
//        Mockito.when(mockSendCommandResult.fetchPage()).thenReturn(mockFetchPageResult);
//
//        final ArgumentCaptor<SendCommandRequest> commandCaptor = ArgumentCaptor.forClass(SendCommandRequest.class);
//        final FetchPageRequest fetchRequest = FetchPageRequest.builder()
//                                                              .transactionId(MOCK_TXN_ID)
//                                                              .nextPageToken(MOCK_NEXT_PAGE_TOKEN)
//                                                              .build();
//        final SendCommandRequest fetchCommand = SendCommandRequest.builder().fetchPage(fetchRequest)
//                                                                  .sessionToken(MOCK_SESSION_TOKEN)
//                                                                  .build();
//
//        final Session session = Session.startSession(MOCK_LEDGER_NAME, mockClient);
//        session.sendFetchPage(MOCK_TXN_ID, MOCK_NEXT_PAGE_TOKEN);
//
//        Mockito.verify(mockClient, Mockito.times(2)).sendCommand(commandCaptor.capture());
//        Mockito.verify(mockSendCommandResult).fetchPage();
//        assertEquals(fetchCommand, commandCaptor.getValue());
//    }
//
//    @Test
//    public void testSendStartTransaction() {
//        Mockito.when(mockSendCommandResult.startTransaction()).thenReturn(mockStartTransactionResult);
//
//        final StartTransactionRequest startTransactionRequest = StartTransactionRequest.builder().build();
//        final SendCommandRequest startTransactionCommand = SendCommandRequest.builder()
//                                                                             .startTransaction(startTransactionRequest)
//                                                                             .sessionToken(MOCK_SESSION_TOKEN)
//                                                                             .build();
//        final ArgumentCaptor<SendCommandRequest> commandCaptor = ArgumentCaptor.forClass(SendCommandRequest.class);
//
//        final Session session = Session.startSession(MOCK_LEDGER_NAME, mockClient);
//        session.sendStartTransaction();
//
//        Mockito.verify(mockClient, Mockito.times(2)).sendCommand(commandCaptor.capture());
//        Mockito.verify(mockSendCommandResult).startTransaction();
//        assertEquals(startTransactionCommand, commandCaptor.getValue());
//    }
//
//    @Test
//    public void testGetId() {
//        final Session session = Session.startSession(MOCK_LEDGER_NAME, mockClient);
//
//        assertEquals(session.getId(), MOCK_REQUEST_ID);
//    }
//
//    @Test
//    public void testGetToken() {
//        final Session session = Session.startSession(MOCK_LEDGER_NAME, mockClient);
//
//        assertEquals(session.getToken(), MOCK_SESSION_TOKEN);
//    }
//
//    @Test
//    public void testInvalidSessionException() {
//        final InvalidSessionException exception = InvalidSessionException.builder().message("").build();
//        final Session session = Session.startSession(MOCK_LEDGER_NAME, mockClient);
//        Mockito.when(mockClient.sendCommand(ArgumentMatchers.any(SendCommandRequest.class)))
//               .thenThrow(exception);
//
//        try {
//            session.sendStartTransaction();
//        } catch (InvalidSessionException ise) {
//            assertEquals(ise, exception);
//        }
//
//        try {
//            session.sendStartTransaction();
//        } catch (InvalidSessionException ise) {
//            assertEquals(ise, exception);
//        }
//        Mockito.verify(mockClient, Mockito.times(3))
//               .sendCommand(ArgumentMatchers.any(SendCommandRequest.class));
//    }
//}
