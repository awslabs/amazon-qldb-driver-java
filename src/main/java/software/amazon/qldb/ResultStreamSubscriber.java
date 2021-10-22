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

    private boolean done;
    private Subscription subscription;
    private final LinkedBlockingQueue<CompletableFuture<ResultStream>> futures;

    protected ResultStreamSubscriber(LinkedBlockingQueue<CompletableFuture<ResultStream>> futures) {
        this.futures = futures;
    }

    @Override
    public void onSubscribe(Subscription s) {
        System.out.println(Thread.currentThread().getName() + " Subscriber: On subscribe ");

        // As per rule 2.13, we need to throw a `java.lang.NullPointerException` if the `Subscription` is `null`
        if (s == null) throw null;

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

        // As per rule 2.13, we need to throw a `java.lang.NullPointerException` if the `element` is `null`
        if (result == null) throw null;

        if (!done) {
            try {
                final CompletableFuture<ResultStream> future = futures.poll(1000L, TimeUnit.MILLISECONDS);
                if (future == null) {
                   throw QldbDriverException.create(Errors.FUTURE_QUEUE_EMTPY.get());
                }

                if (result instanceof TransactionError) {
                    future.completeExceptionally(TransactionException.create((TransactionError)result));
                } else if (result instanceof StatementError) {
                    future.completeExceptionally(StatementException.create((StatementError)result));
                } else future.complete(result);

                subscription.request(1);
            } catch (final Throwable t) {
                done();
                onError(t);
            }
        }
    }

    @Override
    public void onError(Throwable t) {
        System.err.println(Thread.currentThread().getName() + "Subscriber: Error occurred while stream - " + t.getMessage());

        // As per rule 2.13, we need to throw a `java.lang.NullPointerException` if the `Throwable` is `null`
        if (t == null) throw null;

        done();
    }

    @Override
    public void onComplete() {
        System.out.println(Thread.currentThread().getName() + "Subscriber: Finished streaming all events");

        done();
    }

    // Idempotently marking the Subscriber as "done", so we don't want to process more elements
    private void done() {
        //On this line we could add a guard against `!done`, but since rule 3.7 says that `Subscription.cancel()` is idempotent, we don't need to.
        done = true; // If we `whenNext` throws an exception, let's consider ourselves done (not accepting more elements)
        subscription.cancel(); // Cancel the subscription
    }
}