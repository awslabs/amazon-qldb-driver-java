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

package software.amazon.qldbstreaming;

import io.reactivex.rxjava3.subscribers.DefaultSubscriber;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.qldbsessionv2.model.ResultStream;
import software.amazon.awssdk.services.qldbsessionv2.model.StatementError;
import software.amazon.awssdk.services.qldbsessionv2.model.TransactionError;
import software.amazon.qldbstreaming.exceptions.StatementException;
import software.amazon.qldbstreaming.exceptions.TransactionException;

class ResultStreamSubscriber extends DefaultSubscriber<ResultStream> {
    private static final Logger logger = LoggerFactory.getLogger(StreamResult.class);

    private final LinkedBlockingQueue<CompletableFuture<ResultStream>> futures;

    protected ResultStreamSubscriber(LinkedBlockingQueue<CompletableFuture<ResultStream>> futures) {
        this.futures = futures;
    }

    @Override
    public void onNext(ResultStream resultStream) {
        logger.info("Subscriber received result {} from the stream. ", resultStream);

        final CompletableFuture<ResultStream> future;
        try {
            // This could wait forever until there is an incoming request.
            // TODO: We'll have to solve how to timeout the stream in this case,
            // TODO: for example Flowable has a timeout operator.
            future = futures.take();
            if (resultStream instanceof TransactionError) {
                future.completeExceptionally(TransactionException.create((TransactionError) resultStream));
            } else if (resultStream instanceof StatementError) {
                future.completeExceptionally(StatementException.create((StatementError) resultStream));
            } else {
                future.complete(resultStream);
            }
        } catch (InterruptedException e) {
            logger.warn("Errors completing future: {}", e.getMessage(), e);
        }
    }

    @Override
    public void onError(Throwable t) {
        logger.error("Subscriber is terminated by an Error {}", t.getMessage(), t);
        cancel();
    }

    @Override
    public void onComplete() {
        logger.info("Subscriber is terminated successfully");
        cancel();
    }
}