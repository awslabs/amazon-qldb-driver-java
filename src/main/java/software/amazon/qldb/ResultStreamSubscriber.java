package software.amazon.qldb;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import software.amazon.awssdk.services.qldbsessionv2.model.ResultStream;
import software.amazon.awssdk.services.qldbsessionv2.model.StatementError;
import software.amazon.awssdk.services.qldbsessionv2.model.TransactionError;
import software.amazon.qldb.exceptions.Errors;
import software.amazon.qldb.exceptions.QldbDriverException;
import software.amazon.qldb.exceptions.StatementException;
import software.amazon.qldb.exceptions.TransactionException;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

class ResultStreamSubscriber implements Subscriber<ResultStream> {

    private final LinkedBlockingQueue<CompletableFuture<ResultStream>> results;
    private boolean done;
    private Subscription subscription;

    protected ResultStreamSubscriber() {
        this.results = new LinkedBlockingQueue<>();
    }

    CompletableFuture<ResultStream> pollResultFutureFromQueue() throws InterruptedException {
        CompletableFuture<ResultStream> resultFuture = results.poll(1000L, TimeUnit.MILLISECONDS);

        if (resultFuture == null) throw QldbDriverException.create(Errors.RESULT_QUEUE_EMTPY.get());

        return resultFuture.whenComplete((r, e) -> {
            if (e != null) {
                throw QldbDriverException.create(Errors.SUBSCRIBER_TERMINATE.get());
            }

            if (r instanceof TransactionError) {
                // Complete exceptionally if result is a TransactionError.
                // The exception will be handled directly in QldbSession.java.
                throw TransactionException.create((TransactionError)r);
            } else if (r instanceof StatementError) {
                // Complete exceptionally if result is a StatementError.
                // The exception will be handled directly in QldbSession.java.
                throw StatementException.create((StatementError)r);
            }
        });
    }

    @Override
    public void onSubscribe(Subscription s) {
        System.out.println(Thread.currentThread().getName() + " Subscriber: On subscribe ");

        if (s == null) throw QldbDriverException.create(Errors.SUBSCRIBER_ILLEGAL.get());

        if (subscription != null) { // If someone has made a mistake and added this Subscriber multiple times, let's handle it gracefully
            s.cancel(); // Cancel the additional subscription
        } else {
            subscription = s;
            s.request(1);
        }
    }

    @Override
    public void onNext(ResultStream result) {
        System.out.println(Thread.currentThread().getName() + " Subscriber: Received event " + result);

        if (result == null) throw QldbDriverException.create(Errors.SUBSCRIBER_ILLEGAL.get());

        if (!done) { // If we aren't already done
            try {
                CompletableFuture<ResultStream> future = new CompletableFuture<>();
                future.complete(result);
                results.offer(future);
                subscription.request(1);
            } catch (final Throwable t) {
                done();
                onError(t);
            }
        }
    }

    @Override
    public void onError(Throwable t) {
        System.err.println("ResultStreamSubscriber: Error occurred while stream - " + t.getMessage());

        CompletableFuture<ResultStream> future = new CompletableFuture<>();
        future.completeExceptionally(t);
        results.offer(future);
        done();
    }

    @Override
    public void onComplete() {
        System.out.println("ResultStreamSubscriber: Finished streaming all events");

        done();
    }

    // Idempotently marking the Subscriber as "done", so we don't want to process more elements
    private void done() {
        //On this line we could add a guard against `!done`, but since rule 3.7 says that `Subscription.cancel()` is idempotent, we don't need to.
        done = true; // If we `whenNext` throws an exception, let's consider ourselves done (not accepting more elements)
        subscription.cancel(); // Cancel the subscription
    }
}