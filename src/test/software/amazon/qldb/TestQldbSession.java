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

import com.amazon.ion.IonSystem;
import com.amazon.ion.IonValue;
import com.amazon.ion.system.IonSystemBuilder;
import com.amazonaws.AmazonClientException;
import com.amazonaws.services.qldbsession.model.AmazonQLDBSessionException;
import com.amazonaws.services.qldbsession.model.InvalidSessionException;
import com.amazonaws.services.qldbsession.model.OccConflictException;
import com.amazonaws.services.qldbsession.model.RateExceededException;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.http.HttpStatus;
import org.apache.http.NoHttpResponseException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import org.mockito.MockitoAnnotations;
import software.amazon.qldb.exceptions.AbortException;

public class TestQldbSession {
    private static final String LEDGER = "myLedger";
    private static final int RETRY_LIMIT = 3;
    private static final int READ_AHEAD = 0;
    private MockQldbSessionClient client;
    private QldbSessionImpl qldbSession;
    private IonSystem system;
    private List<IonValue> ionList;
    private String statement;
    private Session mockSession;

    @Mock
    private RetryIndicator mockRetryIndicator;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void init() {
        MockitoAnnotations.initMocks(this);

        system = IonSystemBuilder.standard().build();
        ionList  = new ArrayList<>(2);
        ionList.add(system.newString("a"));
        ionList.add(system.newString("b"));
        statement = "select * from test";

        client = new MockQldbSessionClient();
        client.queueResponse(MockResponses.START_SESSION_RESPONSE);
        mockSession = Session.startSession(LEDGER, client);
        qldbSession = new QldbSessionImpl(mockSession, RETRY_LIMIT, READ_AHEAD, system, null);
    }

    @Test
    public void testAbortOrClose() {
        client.queueResponse(MockResponses.ABORT_RESPONSE);
        Assert.assertTrue(qldbSession.abortOrClose());
        Assert.assertFalse(qldbSession.isClosed());
    }

    @Test
    public void testAbortOrCloseWhenClosed() {
        client.queueResponse(MockResponses.endSessionResponse());
        qldbSession.close();
        Assert.assertTrue(qldbSession.isClosed());
        Assert.assertFalse(qldbSession.abortOrClose());
    }

    @Test
    public void testAbortOrCloseFailed() {
        client.queueResponse(new AmazonClientException("an error"));
        Assert.assertFalse(qldbSession.abortOrClose());
        Assert.assertTrue(qldbSession.isClosed());
    }

    // this also tests isClosed()
    @Test
    public void testClose() {
        client.queueResponse(MockResponses.endSessionResponse());
        qldbSession.close();
        Assert.assertTrue(qldbSession.isClosed());
    }

    @Test
    public void testCloseTwice() {
        client.queueResponse(MockResponses.endSessionResponse());
        qldbSession.close();
        qldbSession.close();
        Assert.assertTrue(qldbSession.isClosed());
    }

    @Test
    public void testCloseWhenException() {
        client.queueResponse(new AmazonClientException("msg"));
        qldbSession.close();
        Assert.assertTrue(qldbSession.isClosed());
    }

    @Test
    public void testIsClosedWhenNotClosed() {
        Assert.assertFalse(qldbSession.isClosed());
    }

    @Test
    public void testAutoCloseableWithInvalidSessionException() {
        final InvalidSessionException exception = new InvalidSessionException("msg");
        client.queueResponse(exception);
        client.queueResponse(MockResponses.endSessionResponse());

        thrown.expect(InvalidSessionException.class);
        try (QldbSessionImpl qldbSession = new QldbSessionImpl(mockSession, RETRY_LIMIT, READ_AHEAD, system, null)) {
            qldbSession.startTransaction();
        } finally {
            Assert.assertTrue(client.isQueueEmpty());
        }
    }

    @Test
    public void testGetLedgerName() {
        Assert.assertEquals(LEDGER, qldbSession.getLedgerName());
    }

    @Test
    public void testGetSessionToken() {
        Assert.assertEquals(MockResponses.SESSION_TOKEN, qldbSession.getSessionToken());
    }

    @Test
    public void testGetSessionId() {
        Assert.assertEquals(MockResponses.REQUEST_ID, qldbSession.getSessionId());
    }

    @Test
    public void testStartTransaction() {
        final String id = "id";
        client.queueResponse(MockResponses.startTxnResponse(id));
        final Transaction transaction = qldbSession.startTransaction();
        Assert.assertEquals(id, transaction.getTransactionId());
    }

    @Test
    public void testStartTransactionWhenClosed() {
        client.queueResponse(MockResponses.endSessionResponse());

        thrown.expect(IllegalStateException.class);

        qldbSession.close();
        qldbSession.startTransaction();
    }

    @Test
    public void testStartTransactionError() {
        client.queueResponse(new AmazonClientException("an error"));

        thrown.expect(AmazonClientException.class);
        thrown.expectMessage("an error");

        qldbSession.startTransaction();
    }

    @Test
    public void testStartTransactionInvalidSessionException() {
        final InvalidSessionException exception = new InvalidSessionException("msg");
        client.queueResponse(exception);

        thrown.expect(InvalidSessionException.class);

        qldbSession.startTransaction();
    }

    @Test
    public void testExecuteWithEmptyString() {
        thrown.expect(IllegalArgumentException.class);

        qldbSession.execute("");
    }

    @Test
    public void testExecuteWithErrorAndErrorOnAbort() {
        client.queueResponse(MockResponses.startTxnResponse("id"));
        client.queueResponse(new AmazonClientException("an error1"));
        client.queueResponse(new AmazonClientException("an error2"));

        thrown.expect(AmazonClientException.class);
        try {
            qldbSession.execute(statement);
        } finally {
            Assert.assertTrue(client.isQueueEmpty());
        }
    }

    @Test
    public void testExecuteWithNullString() {
        thrown.expect(IllegalArgumentException.class);

        qldbSession.execute(null, ionList);
    }

    @Test
    public void testExecuteWithListNullParameters() {
        thrown.expect((IllegalArgumentException.class));

        qldbSession.execute(statement, (List<IonValue>) null);
    }

    @Test
    public void testExecuteWithVargNullParameters() {
        thrown.expect((IllegalArgumentException.class));

        qldbSession.execute(statement, (IonValue[]) null);
    }

    @Test
    public void testExecuteWithStatementOnly() throws IOException {
        queueTxnExecCommit(ionList, statement, Collections.emptyList());
        final Result result = qldbSession.execute(statement);
        final Iterator<IonValue> resultIterator = result.iterator();
        final Iterator<IonValue> ionListIterator = ionList.iterator();
        compareIterators(ionListIterator, resultIterator);
    }

    @Test
    public void testExecuteWithStatementAndRetry() throws IOException {
        queueTxnExecCommit(ionList, statement, Collections.emptyList());
        final Result result = qldbSession.execute(statement, mockRetryIndicator);

        verify(mockRetryIndicator, times(0)).onRetry(0);
        final Iterator<IonValue> resultIterator = result.iterator();
        final Iterator<IonValue> ionListIterator = ionList.iterator();
        compareIterators(ionListIterator, resultIterator);
    }

    @Test
    public void testExecuteWithStatementAndListParameters() throws IOException {
        queueTxnExecCommit(ionList, statement, ionList);
        final Result result = qldbSession.execute(statement, ionList);
        final Iterator<IonValue> resultIterator = result.iterator();
        final Iterator<IonValue> ionListIterator = ionList.iterator();
        compareIterators(ionListIterator, resultIterator);
    }

    @Test
    public void testExecuteWithStatementAndListParametersAndRetry() throws IOException {
        queueTxnExecCommit(ionList, statement, ionList);
        final Result result = qldbSession.execute(statement, mockRetryIndicator, ionList);

        verify(mockRetryIndicator, times(0)).onRetry(0);
        final Iterator<IonValue> resultIterator = result.iterator();
        final Iterator<IonValue> ionListIterator = ionList.iterator();
        compareIterators(ionListIterator, resultIterator);
    }

    @Test
    public void testExecuteWithStatementAndEmptyArgParameters() throws IOException {
        final IonValue[] ionParameters = new IonValue[0];
        final List<IonValue> emptyIonList = Collections.emptyList();
        queueTxnExecCommit(emptyIonList, statement, emptyIonList);
        final Result result = qldbSession.execute(statement, ionParameters);
        final Iterator<IonValue> resultIterator = result.iterator();
        final Iterator<IonValue> ionListIterator = emptyIonList.iterator();
        compareIterators(ionListIterator, resultIterator);
    }

    @Test
    public void testExecuteWithStatementAndOneArgParameters() throws IOException {
        final List<IonValue> singleValIonList = new ArrayList<>();
        singleValIonList.add(system.newString("a"));
        queueTxnExecCommit(singleValIonList, statement, singleValIonList);
        final Result result = qldbSession.execute(statement, singleValIonList.get(0));
        final Iterator<IonValue> resultIterator = result.iterator();
        final Iterator<IonValue> ionListIterator = singleValIonList.iterator();
        compareIterators(ionListIterator, resultIterator);
    }

    @Test
    public void testExecuteWithStatementAndManyArgParameters() throws IOException {
        queueTxnExecCommit(ionList, statement, ionList);
        final Result result = qldbSession.execute(statement, ionList.get(0), ionList.get(1));
        final Iterator<IonValue> resultIterator = result.iterator();
        final Iterator<IonValue> ionListIterator = ionList.iterator();
        compareIterators(ionListIterator, resultIterator);
    }

    @Test
    public void testExecuteWithStatementAndArgParametersAndRetry() throws IOException {
        final List<IonValue> singleValIonList = new ArrayList<>();
        singleValIonList.add(system.newString("a"));
        queueTxnExecCommit(singleValIonList, statement, singleValIonList);
        final Result result = qldbSession.execute(statement, mockRetryIndicator, singleValIonList.get(0));

        verify(mockRetryIndicator, times(0)).onRetry(0);
        final Iterator<IonValue> resultIterator = result.iterator();
        final Iterator<IonValue> ionListIterator = singleValIonList.iterator();
        compareIterators(ionListIterator, resultIterator);
    }

    @Test
    public void testExecuteWithOccConflict() throws IOException {
        // Add one more error response than the number of configured OCC retries.
        thrown.expect(OccConflictException.class);

        for (int i = 0; i < RETRY_LIMIT + 1; ++i) {
            queueTxnExecError("id" + i);
        }
        qldbSession.execute(statement);
    }

    @Test
    public void testExecuteWithOneUnsuccessfulOccAttempt() throws IOException {
        queueTxnExecError();
        queueTxnExecCommit(ionList, statement, ionList);

        final Result result = qldbSession.execute(statement, ionList);
        final Iterator<IonValue> resultIterator = result.iterator();
        final Iterator<IonValue> ionListIterator = ionList.iterator();
        compareIterators(ionListIterator, resultIterator);
    }

    @Test
    public void testExecuteExecutorLambdaWithNoReturnValue() throws IOException {
        queueTxnExecCommit(ionList, statement, Collections.emptyList());

        qldbSession.execute(txnExecutor -> {
            Result result = txnExecutor.execute(statement);
        }, mockRetryIndicator);
        verify(mockRetryIndicator, times(0)).onRetry(0);
    }

    @Test
    public void testExecuteExecutorLambdaWithNoReturnValueNoRetry() throws IOException {
        queueTxnExecCommit(ionList, statement, Collections.emptyList());
        final AtomicBoolean executedFlag = new AtomicBoolean(false);

        qldbSession.execute(txnExecutor -> {
            Result result = txnExecutor.execute(statement);
            executedFlag.set(true);
        });
        Assert.assertTrue(executedFlag.get());
    }

    @Test
    public void testExecuteExecutorLambdaWithNoReturnValueOccConflict() throws IOException {
        // Add one more error response than the number of configured OCC retries.
        thrown.expect(OccConflictException.class);

        for (int i = 0; i < RETRY_LIMIT + 1; ++i) {
            queueTxnExecError("id" + i);
        }
        try {
            qldbSession.execute(txnExecutor -> {
                Result result = txnExecutor.execute(statement);
            }, mockRetryIndicator);
        } finally {
            verify(mockRetryIndicator, times(1)).onRetry(1);
            verify(mockRetryIndicator, times(1)).onRetry(2);
            verify(mockRetryIndicator, times(1)).onRetry(RETRY_LIMIT);
            verify(mockRetryIndicator, times(0)).onRetry(RETRY_LIMIT + 1);
        }
    }

    @Test
    public void testExecuteExecutorLambdaWithQldbSessionExceptions() throws IOException {
        final AmazonQLDBSessionException exception1 = new AmazonQLDBSessionException("1");
        exception1.setStatusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR);
        // This should retry.
        queueTxnExecError(exception1);
        final AmazonQLDBSessionException exception2 = new AmazonQLDBSessionException("2");
        exception2.setStatusCode(HttpStatus.SC_SERVICE_UNAVAILABLE);
        // This should retry.
        queueTxnExecError(exception2);
        final AmazonQLDBSessionException exception3 = new AmazonQLDBSessionException("3");
        exception3.setStatusCode(HttpStatus.SC_NOT_FOUND);
        // This should throw.
        queueTxnExecError(exception3);

        thrown.expect(AmazonQLDBSessionException.class);

        try {
            qldbSession.execute(txnExecutor -> {
                Result result = txnExecutor.execute(statement);
            }, mockRetryIndicator);
        } finally {
            verify(mockRetryIndicator, times(1)).onRetry(1);
            verify(mockRetryIndicator, times(1)).onRetry(2);
            verify(mockRetryIndicator, times(0)).onRetry(RETRY_LIMIT);
        }
    }

    @Test
    public void testExecuteExecutorLambdaWithQldbSessionExceptionsExceedRetry() throws IOException {
        thrown.expect(AmazonQLDBSessionException.class);

        for (int i = 0; i < RETRY_LIMIT + 1; ++i) {
            final AmazonQLDBSessionException exception = new AmazonQLDBSessionException("msg");
            exception.setStatusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR);
            queueTxnExecError(exception);
        }

        try {
            qldbSession.execute(txnExecutor -> {
                Result result = txnExecutor.execute(statement);
            }, mockRetryIndicator);
        } finally {
            verify(mockRetryIndicator, times(1)).onRetry(1);
            verify(mockRetryIndicator, times(1)).onRetry(2);
            verify(mockRetryIndicator, times(1)).onRetry(RETRY_LIMIT);
            verify(mockRetryIndicator, times(0)).onRetry(RETRY_LIMIT + 1);
        }
    }

    @Test
    public void testExecuteExecutorLambdaWithAmazonClientExceptions() throws IOException {
        final AmazonClientException exception1 = new AmazonClientException("1", new NoHttpResponseException("3"));
        // This should retry.
        queueTxnExecError(exception1);
        final AmazonClientException exception2 = new AmazonClientException("2", new SocketTimeoutException("2"));
        // This should retry.
        queueTxnExecError(exception2);
        final RateExceededException exception3 = new RateExceededException("3");
        // This should throw.
        queueTxnExecError(exception3);

        thrown.expect(RateExceededException.class);

        try {
            qldbSession.execute(txnExecutor -> {
                Result result = txnExecutor.execute(statement);
            }, mockRetryIndicator);
        } finally {
            verify(mockRetryIndicator, times(1)).onRetry(1);
            verify(mockRetryIndicator, times(1)).onRetry(2);
            verify(mockRetryIndicator, times(0)).onRetry(RETRY_LIMIT);
        }
    }

    @Test
    public void testExecuteExecutorLambdaWithAmazonClientExceptionsExceedRetry() throws IOException {
        for (int i = 0; i < RETRY_LIMIT + 1; ++i) {
            final AmazonClientException exception = new AmazonClientException("msg", new NoHttpResponseException("cause"));
            queueTxnExecError(exception);
        }

        thrown.expect(AmazonClientException.class);

        try {
            qldbSession.execute(txnExecutor -> {
                Result result = txnExecutor.execute(statement);
            }, mockRetryIndicator);
        } finally {
            verify(mockRetryIndicator, times(1)).onRetry(1);
            verify(mockRetryIndicator, times(1)).onRetry(2);
            verify(mockRetryIndicator, times(1)).onRetry(RETRY_LIMIT);
            verify(mockRetryIndicator, times(0)).onRetry(RETRY_LIMIT + 1);
        }
    }

    @Test
    public void testExecuteExecutorLambdaWithInvalidSessionExceptionsExceedRetry() throws IOException {
        for (int i = 0; i < RETRY_LIMIT + 1; ++i) {
            final InvalidSessionException exception = new InvalidSessionException("");
            queueTxnExecError(exception);
            client.queueResponse(MockResponses.START_SESSION_RESPONSE);
        }

        thrown.expect(InvalidSessionException.class);

        try {
            qldbSession.execute(txnExecutor -> {
                Result result = txnExecutor.execute(statement);
            }, mockRetryIndicator);
        } finally {
            verify(mockRetryIndicator, times(1)).onRetry(1);
            verify(mockRetryIndicator, times(1)).onRetry(2);
            verify(mockRetryIndicator, times(1)).onRetry(RETRY_LIMIT);
            verify(mockRetryIndicator, times(0)).onRetry(RETRY_LIMIT + 1);
        }
    }

    @Test
    public void testExecuteExecutorLambdaWithInvalidSessionException() throws IOException {
        final InvalidSessionException exception = new InvalidSessionException("");
        queueTxnExecError(exception);
        client.queueResponse(MockResponses.START_SESSION_RESPONSE);
        queueTxnExecCommit(ionList, statement, Collections.emptyList());

        qldbSession.execute(txnExecutor -> {
            Result result = txnExecutor.execute(statement);
        }, mockRetryIndicator);
        verify(mockRetryIndicator, times(1)).onRetry(1);
    }


    @Test
    public void testExecuteExecutorLambdaWithReturnValue() throws IOException {
        queueTxnExecCommit(ionList, statement, Collections.emptyList());

        final Result result = qldbSession.execute(txnExecutor -> {
            Result res = txnExecutor.execute(statement);
            return new BufferedResult(res);
        }, mockRetryIndicator);

        verify(mockRetryIndicator, times(0)).onRetry(0);
        final Iterator<IonValue> resultIterator = result.iterator();
        final Iterator<IonValue> ionListIterator = ionList.iterator();
        compareIterators(ionListIterator, resultIterator);
    }

    @Test
    public void testExecuteExecutorLambdaWithReturnValueNoRetry() throws IOException {
        queueTxnExecCommit(ionList, statement, Collections.emptyList());

        final Result result = qldbSession.execute(txnExecutor -> {
            Result res = txnExecutor.execute(statement);
            return new BufferedResult(res);
        });

        final Iterator<IonValue> resultIterator = result.iterator();
        final Iterator<IonValue> ionListIterator = ionList.iterator();
        compareIterators(ionListIterator, resultIterator);
    }

    @Test
    public void testExecuteExecutorLambdaWithReturnValueOccConflict() throws IOException {
        // Add one more error response than the number of configured OCC retries.
        thrown.expect(OccConflictException.class);

        for (int i = 0; i < RETRY_LIMIT + 1; ++i) {
            queueTxnExecError("id" + i);
        }
        try {
            qldbSession.execute(txnExecutor -> {
                Result res = txnExecutor.execute(statement);
                return new BufferedResult(res);
            }, mockRetryIndicator);
        } finally {
            verify(mockRetryIndicator, times(1)).onRetry(1);
            verify(mockRetryIndicator, times(1)).onRetry(2);
            verify(mockRetryIndicator, times(1)).onRetry(RETRY_LIMIT);
            verify(mockRetryIndicator, times(0)).onRetry(RETRY_LIMIT + 1);
        }
    }

    @Test
    public void testExecuteExecutorLambdaWithNonBufferedResultAndReturnValue() throws IOException {
        queueTxnExecCommit(ionList, statement, Collections.emptyList());

        final Result result = qldbSession.execute(txnExecutor -> {
            return txnExecutor.execute(statement);
        }, mockRetryIndicator);

        Assert.assertTrue(result instanceof BufferedResult);
        verify(mockRetryIndicator, times(0)).onRetry(0);
        final Iterator<IonValue> resultIterator = result.iterator();
        final Iterator<IonValue> ionListIterator = ionList.iterator();
        compareIterators(ionListIterator, resultIterator);
    }

    @Test
    public void testExecuteExecutorLambdaWithClosedNonBufferedResultAndReturnValue() throws IOException {
        queueTxnExecCommit(ionList, statement, Collections.emptyList());

        qldbSession.execute(txnExecutor -> {
            return txnExecutor.execute(statement);
        }, mockRetryIndicator);
        verify(mockRetryIndicator, times(0)).onRetry(0);
    }

    @Test
    public void testExecuteExecutorLambdaWithReturnValueOneUnsuccessfulOccAttempt() throws IOException {
        queueTxnExecError("id1");
        queueTxnExecCommit(ionList, statement, Collections.emptyList());

        final Result result = qldbSession.execute(txnExecutor -> {
            Result res = txnExecutor.execute(statement);
            return new BufferedResult(res);
        }, mockRetryIndicator);

        verify(mockRetryIndicator, times(1)).onRetry(1);
        final Iterator<IonValue> resultIterator = result.iterator();
        final Iterator<IonValue> ionListIterator = ionList.iterator();
        compareIterators(ionListIterator, resultIterator);
    }

    @Test
    public void testExecuteExecutorLambdaWithNullSuccessfulCallback() throws IOException {
        queueTxnExecCommit(ionList, statement, Collections.emptyList());

        final Result result = qldbSession.execute(txnExecutor -> {
            Result res = txnExecutor.execute(statement);
            return new BufferedResult(res);
        }, null);

        final Iterator<IonValue> resultIterator = result.iterator();
        final Iterator<IonValue> ionListIterator = ionList.iterator();
        compareIterators(ionListIterator, resultIterator);
    }

    @Test
    public void testExecuteWhenClosed() {
        client.queueResponse(MockResponses.endSessionResponse());

        thrown.expect(IllegalStateException.class);

        qldbSession.close();
        qldbSession.execute(statement);
    }

    @Test
    public void testExecuteExecutorLambdaWhenClosed() {
        client.queueResponse(MockResponses.endSessionResponse());

       thrown.expect(IllegalStateException.class);

        qldbSession.close();
        qldbSession.execute(txnExecutor -> {
            return txnExecutor.execute(statement);
        }, null);
    }

    @Test
    public void testInternalExecuteWithNullExecutor() throws IOException {
        queueTxnExecCommit(ionList, statement, Collections.emptyList());
        final Executor<Boolean> exec = null;

        thrown.expect(IllegalArgumentException.class);

        qldbSession.execute(exec, mockRetryIndicator);
        verify(mockRetryIndicator, times(0)).onRetry(0);
    }

    @Test
    public void testInternalExecuteWithAbortedTransaction() throws IOException {
        client.queueResponse(MockResponses.startTxnResponse("id"));
        client.queueResponse(MockResponses.executeResponse(ionList));
        client.queueResponse(MockResponses.ABORT_RESPONSE);

        thrown.expect(AbortException.class);

        qldbSession.execute(txnExecutor -> {
            Result res = txnExecutor.execute(statement);
            BufferedResult bufferedResult = new BufferedResult(res);
            txnExecutor.abort();
            return bufferedResult;
        }, mockRetryIndicator);

        verify(mockRetryIndicator, times(0)).onRetry(0);
    }

    @Test
    public void testInternalExecuteWithError() {
        client.queueResponse(MockResponses.startTxnResponse("id"));
        client.queueResponse(new AmazonClientException("an Error1"));
        client.queueResponse(MockResponses.ABORT_RESPONSE);

        thrown.expect(AmazonClientException.class);

        try {
            qldbSession.execute(txn -> { txn.execute(statement); }, null);
        } finally {
            Assert.assertTrue(client.isQueueEmpty());
        }
    }

    @Test
    public void testInternalExecuteWithErrorAndErrorOnAbort() {
        client.queueResponse(MockResponses.startTxnResponse("id"));
        client.queueResponse(new AmazonClientException("an Error1"));
        client.queueResponse(new AmazonClientException("an Error2"));

        thrown.expect(AmazonClientException.class);

        try {
            qldbSession.execute(txn -> { txn.execute(statement); }, null);
        } finally {
            Assert.assertTrue(client.isQueueEmpty());
        }
    }

    @Test
    public void testGetTableNames() throws IOException {
        final List<String> tables = Arrays.asList("table1", "table2");
        final List<IonValue> ionTables = tables.stream().map(system::newString).collect(Collectors.toList());

        queueTxnExecCommit(ionTables, QldbSessionImpl.TABLE_NAME_QUERY, Collections.emptyList());

        final Iterable<String> result = qldbSession.getTableNames();
        final Iterator<String> resultIterator = result.iterator();
        final Iterator<String> tableIterator = tables.iterator();
        compareIterators(tableIterator, resultIterator);
    }

    @Test
    public void testGetTableNamesOccConflict() throws IOException {
        final List<String> tables = Arrays.asList("table1", "table2");
        final List<IonValue> ionTables = tables.stream().map(system::newString).collect(Collectors.toList());

        // Add one more error response than the number of configured OCC retries.
        thrown.expect(OccConflictException.class);

        for (int i = 0; i < RETRY_LIMIT + 1; ++i) {
            client.queueResponse(MockResponses.startTxnResponse("id" + i));
            client.queueResponse(MockResponses.executeResponse(ionTables));
            client.queueResponse(new OccConflictException("msg"));
        }
        qldbSession.getTableNames();
    }

    @Test
    public void testGetTableNamesWhenClosed() {
        client.queueResponse(MockResponses.endSessionResponse());

        thrown.expect(IllegalStateException.class);

        qldbSession.close();
        qldbSession.getTableNames();
    }

    private static <E> void compareIterators(Iterator<E> iterator1, Iterator<E> iterator2) {
        while (iterator2.hasNext() || iterator1.hasNext()) {
            Assert.assertEquals(iterator2.hasNext(), iterator1.hasNext());
            Assert.assertEquals(iterator1.next(), iterator2.next());
        }
    }

    private void queueTxnExecCommit(List<IonValue> values, String statement, List<IonValue> parameters) throws IOException {
        String txnId = "id";
        QldbHash txnHash = QldbHash.toQldbHash(txnId, system);
        txnHash = BaseTransaction.dot(txnHash, statement, parameters, system);
        client.queueResponse(MockResponses.startTxnResponse(txnId));
        client.queueResponse(MockResponses.executeResponse(values));
        client.queueResponse(MockResponses.commitTransactionResponse(ByteBuffer.wrap(txnHash.getQldbHash())));
    }

    private void queueTxnExecError() throws IOException {
        queueTxnExecError("id");
    }

    private void queueTxnExecError(String id) throws IOException {
        client.queueResponse(MockResponses.startTxnResponse(id));
        client.queueResponse(MockResponses.executeResponse(ionList));
        client.queueResponse(new OccConflictException("msg"));
    }

    public void queueTxnExecError(AmazonClientException ace) throws IOException {
        client.queueResponse(MockResponses.startTxnResponse("id"));
        client.queueResponse(MockResponses.executeResponse(ionList));
        client.queueResponse(ace);
        client.queueResponse(MockResponses.ABORT_RESPONSE);
    }
}
