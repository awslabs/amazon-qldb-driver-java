/*
 * Copyright 2014-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with
 * the License. A copy of the License is located at
 *
 * http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */
package software.amazon.qldb;

import java.util.ArrayDeque;
import java.util.Queue;

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.ResponseMetadata;
import com.amazonaws.services.qldbsession.AmazonQLDBSession;
import com.amazonaws.services.qldbsession.model.SendCommandRequest;
import com.amazonaws.services.qldbsession.model.SendCommandResult;

public class MockQldbSessionClient implements AmazonQLDBSession {
    private static class Holder {
        public final SendCommandResult result;
        public final RuntimeException exception;

        public Holder(SendCommandResult result) {
            this.result = result;
            this.exception = null;
        }

        public Holder(RuntimeException e) {
            this.result = null;
            this.exception = e;
        }
    }

    private final Queue<Holder> resultQueue;

    public MockQldbSessionClient() {
        resultQueue = new ArrayDeque<>();
    }

    @Override
    public SendCommandResult sendCommand(SendCommandRequest sendCommandRequest) {
        final Holder response = resultQueue.remove();
        if (response.exception != null) {
            throw response.exception;
        }

        return response.result;
    }

    @Override
    public void shutdown() {

    }

    @Override
    public ResponseMetadata getCachedResponseMetadata(AmazonWebServiceRequest amazonWebServiceRequest) {
        return null;
    }

    public boolean isQueueEmpty() {
        return resultQueue.isEmpty();
    }

    public void queueResponse(SendCommandResult response) {
        resultQueue.add(new Holder(response));
    }

    public void queueResponse(RuntimeException e) {
        resultQueue.add(new Holder(e));
    }
}
