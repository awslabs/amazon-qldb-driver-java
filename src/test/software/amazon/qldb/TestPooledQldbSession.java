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
import com.amazonaws.services.qldbsession.model.InvalidSessionException;
import java.util.Collections;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

public class TestPooledQldbSession {
    @Mock
    QldbSessionImpl mockSession;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    PooledQldbSession session;

    boolean wasCloseCalled;

    @Before
    public void init() {
        MockitoAnnotations.initMocks(this);

        session = new PooledQldbSession(mockSession, this::verifyCloseSession);
        wasCloseCalled = false;
    }

    @Test
    public void testClose() {
        session.close();
        Assert.assertTrue(wasCloseCalled);
    }

    @Test
    public void testCloseWithUsing() {
        try (PooledQldbSession session = new PooledQldbSession(mockSession, this::verifyCloseSession)) {
        }
        Assert.assertTrue(wasCloseCalled);
    }

    @Test
    public void testAutoCloseableWithInvalidSessionException() {
        Mockito.when(mockSession.startTransaction()).thenThrow(new InvalidSessionException(""));

        thrown.expect(InvalidSessionException.class);

        try (PooledQldbSession session = new PooledQldbSession(mockSession, this::verifyCloseSession)) {
            session.startTransaction();
        } finally {
            Assert.assertTrue(wasCloseCalled);
        }
    }

    @Test
    public void testExecuteWithStatement() {
        session.execute("query");
        Mockito.verify(mockSession).execute("query");
    }

    @Test
    public void TestExecuteWithStatementAndRetry() {
        session.execute("query", (RetryIndicator) null);
        Mockito.verify(mockSession).execute("query", (RetryIndicator) null);
    }

    @Test
    public void testExecuteWithListParameters() {
        session.execute("query", Collections.emptyList());
        Mockito.verify(mockSession).execute("query", Collections.emptyList());
    }

    @Test
    public void TestExecuteWithStatementAndListParametersAndRetry() {
        session.execute("query", null, Collections.emptyList());
        Mockito.verify(mockSession).execute("query", null, Collections.emptyList());
    }

    @Test
    public void testExecuteWithArgParameters() {
        session.execute("query", new IonValue[0]);
        Mockito.verify(mockSession).execute("query", new IonValue[0]);
    }

    @Test
    public void TestExecuteWithStatementAndArgParametersAndRetry() {
        session.execute("query", null, new IonValue[0]);
        Mockito.verify(mockSession).execute("query", null, new IonValue[0]);
    }

    @Test
    public void testExecuteLambdaNoRetry() {
        session.execute(txn -> {});
        Mockito.verify(mockSession).execute(ArgumentMatchers.any(ExecutorNoReturn.class));
    }

    @Test
    public void testExecuteLambdaNoReturn() {
        session.execute(txn -> {}, null);
        Mockito.verify(mockSession).execute(ArgumentMatchers.any(ExecutorNoReturn.class), ArgumentMatchers.isNull());
    }

    @Test
    public void testExecuteLambdaReturnNoRetry() {
        session.execute(txn -> 5);
        Mockito.verify(mockSession).execute(ArgumentMatchers.any(Executor.class));
    }

    @Test
    public void testExecuteLambdaReturn() {
        session.execute(txn -> 5, null);
        Mockito.verify(mockSession).execute(ArgumentMatchers.any(Executor.class), ArgumentMatchers.isNull());
    }

    @Test
    public void testGetLedgerName() {
        session.getLedgerName();
        Mockito.verify(mockSession).getLedgerName();
    }

    @Test
    public void testGetSessionToken() {
        session.getSessionToken();
        Mockito.verify(mockSession).getSessionToken();
    }

    @Test (expected = IllegalStateException.class)
    public void testGetSessionWhenClosed() {
        session.close();
        session.getSessionToken();
    }

    @Test
    public void testGetTableNames() {
        session.getTableNames();
        Mockito.verify(mockSession).getTableNames();
    }

    @Test
    public void testStartTransaction() {
        session.startTransaction();
        Mockito.verify(mockSession).startTransaction();
    }

    @Test
    public void testThrowIfClosed() {
        thrown.expect(IllegalStateException.class);

        session.close();
        session.startTransaction();
    }

    void verifyCloseSession(QldbSessionImpl session) {
        Assert.assertEquals(mockSession, session);
        wasCloseCalled = true;
    }
}
