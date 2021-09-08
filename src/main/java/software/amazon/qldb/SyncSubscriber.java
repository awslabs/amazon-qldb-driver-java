package software.amazon.qldb;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

public abstract class SyncSubscriber<T> implements Subscriber<T> {
    Subscription subscription;
    private boolean done;

    @Override
    public void onSubscribe(Subscription s) {
        System.out.println(Thread.currentThread().getName() + " Subscriber: On subscribe ");

        // As per rule 2.13, we need to throw a `java.lang.NullPointerException` if the `Subscription` is `null`
        if (s == null) throw null;

        if (subscription != null) { // If someone has made a mistake and added this Subscriber multiple times, let's handle it gracefully
            try {
                s.cancel(); // Cancel the additional subscription
            } catch(final Throwable t) {
                //Subscription.cancel is not allowed to throw an exception, according to rule 3.15
                (new IllegalStateException(s + " violated the Reactive Streams rule 3.15 by throwing an exception from cancel.", t)).printStackTrace(System.err);
            }
        } else {
            // We have to assign it locally before we use it, if we want to be a synchronous `Subscriber`
            // Because according to rule 3.10, the Subscription is allowed to call `onNext` synchronously from within `request`
            subscription = s;
            try {
                // If we want elements, according to rule 2.1 we need to call `request`
                // And, according to rule 3.2 we are allowed to call this synchronously from within the `onSubscribe` method
                s.request(1); // Our Subscriber is unbuffered and modest, it requests one element at a time
            } catch(final Throwable t) {
                // Subscription.request is not allowed to throw according to rule 3.16
                (new IllegalStateException(s + " violated the Reactive Streams rule 3.16 by throwing an exception from request.", t)).printStackTrace(System.err);
            }
        }
    }

    @Override
    public void onNext(T element) {
        System.out.println(Thread.currentThread().getName() + " Subscriber: Received event " + element);

        if (subscription == null) { // Technically this check is not needed, since we are expecting Publishers to conform to the spec
            (new IllegalStateException("Publisher violated the Reactive Streams rule 1.09 signalling onNext prior to onSubscribe.")).printStackTrace(System.err);
        } else {
            // As per rule 2.13, we need to throw a `java.lang.NullPointerException` if the `resultStream` is `null`
            if (element == null) throw null;

            if (!done) { // If we aren't already done
                try {
                    try {
                        whenReceived(element);
                    } catch (final Throwable t) {
                            // Subscription.request is not allowed to throw according to rule 3.16
                        (new IllegalStateException(subscription + " violated the Reactive Streams rule 3.16 by throwing an exception from request.", t)).printStackTrace(System.err);
                    }
                } catch (final Throwable t) {
                    done();
                    try {
                        onError(t);
                    } catch (final Throwable t2) {
                        //Subscriber.onError is not allowed to throw an exception, according to rule 2.13
                        (new IllegalStateException(this + " violated the Reactive Streams rule 2.13 by throwing an exception from onError.", t2)).printStackTrace(System.err);
                    }
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
            // As per rule 2.13, we need to throw a `java.lang.NullPointerException` if the `Throwable` is `null`
            if (t == null) throw null;
            // Here we are not allowed to call any methods on the `Subscription` or the `Publisher`, as per rule 2.3
            // And anyway, the `Subscription` is considered to be cancelled if this method gets called, as per rule 2.4
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

    // Called in onNext()
    protected abstract void whenReceived(final T element) throws InterruptedException;

    // Idempotently marking the Subscriber as "done", so we don't want to process more elements
    private void done() {
        //On this line we could add a guard against `!done`, but since rule 3.7 says that `Subscription.cancel()` is idempotent, we don't need to.
        done = true; // If we `whenNext` throws an exception, let's consider ourselves done (not accepting more elements)
        try {
            subscription.cancel(); // Cancel the subscription
        } catch(final Throwable t) {
            //Subscription.cancel is not allowed to throw an exception, according to rule 3.15
            (new IllegalStateException(subscription + " violated the Reactive Streams rule 3.15 by throwing an exception from cancel.", t)).printStackTrace(System.err);
        }
    }
}
