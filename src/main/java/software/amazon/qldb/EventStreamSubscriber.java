package software.amazon.qldb;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import software.amazon.awssdk.services.qldbsessionv2.model.ResultStream;
import software.amazon.qldb.exceptions.Errors;
import software.amazon.qldb.exceptions.QldbDriverException;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

class EventStreamSubscriber implements Subscriber<ResultStream> {

    private final LinkedBlockingQueue<ResultStream> results;
    private boolean done;
    private Subscription subscription;

    protected EventStreamSubscriber() {
        this.results = new LinkedBlockingQueue<>();
    }

    ResultStream waitForResult() throws InterruptedException {
        return results.poll(1000L, TimeUnit.MILLISECONDS);
    }

    @Override
    public void onSubscribe(Subscription s) {
        System.out.println(Thread.currentThread().getName() + " Subscriber: On subscribe ");

        if (s == null) throw QldbDriverException.create(Errors.SUBSCRIBER_ILLEGAL.get());

        if (subscription != null) { // If someone has made a mistake and added this Subscriber multiple times, let's handle it gracefully
            s.cancel(); // Cancel the additional subscription
        } else {
            // We have to assign it locally before we use it, if we want to be a synchronous `Subscriber`
            // Because according to rule 3.10, the Subscription is allowed to call `onNext` synchronously from within `request`
            subscription = s;
            // If we want elements, according to rule 2.1 we need to call `request`
            // And, according to rule 3.2 we are allowed to call this synchronously from within the `onSubscribe` methods.request(1);
            // Our Subscriber is unbuffered and modest, it requests one element at a time
            s.request(1);
        }
    }

    @Override
    public void onNext(ResultStream element) {
        System.out.println(Thread.currentThread().getName() + " Subscriber: Received event " + element);

        if (subscription == null) { // Technically this check is not needed, since we are expecting Publishers to conform to the spec
            (new IllegalStateException("Publisher violated the Reactive Streams rule 1.09 signalling onNext prior to onSubscribe.")).printStackTrace(System.err);
        } else {
            if (element == null) throw QldbDriverException.create(Errors.SUBSCRIBER_ILLEGAL.get());

            if (!done) { // If we aren't already done
                try {
                    results.offer(element);
                    subscription.request(1);
                } catch (final Throwable t) {
                    done();
                    onError(t);
                }
            }
        }
    }

    @Override
    public void onError(Throwable t) {
        System.err.println("ResultStreamSubscriber: Error occurred while stream - " + t.getMessage());

        if (subscription == null) { // Technically this check is not needed, since we are expecting Publishers to conform to the spec
            (new IllegalStateException("Publisher violated the Reactive Streams rule 1.09 signalling onError prior to onSubscribe.")).printStackTrace(System.err);
        } else {
            // Here we are not allowed to call any methods on the `Subscription` or the `Publisher`, as per rule 2.3
            // And anyway, the `Subscription` is considered to be cancelled if this method gets called, as per rule 2.4
            done();
        }

    }

    @Override
    public void onComplete() {
        System.out.println("ResultStreamSubscriber: Finished streaming all events");

        if (subscription == null) { // Technically this check is not needed, since we are expecting Publishers to conform to the spec
            (new IllegalStateException("Publisher violated the Reactive Streams rule 1.09 signalling onComplete prior to onSubscribe.")).printStackTrace(System.err);
        } else {
            // Here we are not allowed to call any methods on the `Subscription` or the `Publisher`, as per rule 2.3
            // And anyway, the `Subscription` is considered to be cancelled if this method gets called, as per rule 2.4
            done();
        }
    }

    // Idempotently marking the Subscriber as "done", so we don't want to process more elements
    private void done() {
        //On this line we could add a guard against `!done`, but since rule 3.7 says that `Subscription.cancel()` is idempotent, we don't need to.
        done = true; // If we `whenNext` throws an exception, let's consider ourselves done (not accepting more elements)
        subscription.cancel(); // Cancel the subscription
    }
}