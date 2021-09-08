package software.amazon.qldb;

import software.amazon.awssdk.services.qldbsessionv2.model.CommandResult;
import software.amazon.awssdk.services.qldbsessionv2.model.ResultStream;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

class ResultStreamSubscriber extends SyncSubscriber<ResultStream> {

    private final LinkedBlockingQueue<CommandResult> commandResults;

    protected ResultStreamSubscriber() {
        this.commandResults = new LinkedBlockingQueue<>();
    }

    CommandResult waitForResult() throws InterruptedException {
        CommandResult result = commandResults.poll(5000L, TimeUnit.MILLISECONDS);
        if (result == null) {
            System.out.println(Thread.currentThread().getName() + "Timeout waiting for result.");
        }
        System.out.println(Thread.currentThread().getName() + " Result Stream Subscriber: poll response " + result + " from queue");
        return result;
    }


    protected void whenReceived(ResultStream resultStream) {
        CommandResult commandResult = (CommandResult) resultStream;

        if (commandResult.fetchPage() != null) {

        }

        commandResults.offer(commandResult);
        subscription.request(1);

    }

}
